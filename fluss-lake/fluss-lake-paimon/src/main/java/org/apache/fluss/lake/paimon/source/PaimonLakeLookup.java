/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.lake.source.LakeLookup;
import org.apache.fluss.row.InternalRow;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonLiteral;

/** Paimon primary-key lookup backed by {@link LocalTableQuery}. */
public class PaimonLakeLookup implements LakeLookup<PaimonSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeLookup.class);

    private final LocalTableQuery tableQuery;
    private final IOManager ioManager;
    private final RowType paimonRowType;
    private final InternalRowSerializer partitionSerializer;
    private final RowKeyExtractor rowKeyExtractor;
    private final boolean projected;
    private final Map<PaimonPartitionBucket, Map<String, DataFileMeta>> loadedFiles =
            new HashMap<>();

    public PaimonLakeLookup(FileStoreTable fileStoreTable, @Nullable int[][] project) {
        this(fileStoreTable, project, null);
    }

    public PaimonLakeLookup(
            FileStoreTable fileStoreTable,
            @Nullable int[][] project,
            @Nullable String ioManagerTmpDir) {
        if (fileStoreTable.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Paimon lake lookup only supports primary-key tables.");
        }
        this.ioManager = createIOManager(ioManagerTmpDir);
        this.paimonRowType = fileStoreTable.rowType();
        this.partitionSerializer =
                new InternalRowSerializer(fileStoreTable.schema().logicalPartitionType());
        this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
        this.projected = project != null;
        this.tableQuery = fileStoreTable.newLocalTableQuery().withIOManager(ioManager);
        if (project != null) {
            int[] projectIds = Arrays.stream(project).mapToInt(nested -> nested[0]).toArray();
            tableQuery.withValueProjection(projectIds);
        }
    }

    @Override
    public synchronized void refresh(List<PaimonSplit> splits) {
        Map<PaimonPartitionBucket, Map<String, DataFileMeta>> plannedFiles = new HashMap<>();
        for (PaimonSplit split : splits) {
            Map<String, DataFileMeta> files =
                    plannedFiles.computeIfAbsent(
                            createPartitionBucket(split), ignored -> new HashMap<>());
            for (DataFileMeta file : split.dataSplit().dataFiles()) {
                files.put(file.fileName(), file);
            }
        }

        for (Map.Entry<PaimonPartitionBucket, Map<String, DataFileMeta>> entry :
                plannedFiles.entrySet()) {
            refreshFiles(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized @Nullable InternalRow lookup(
            List<String> partitionValues,
            int bucket,
            Object[] primaryKeyValues,
            int[] primaryKeyIndexes)
            throws IOException {
        GenericRow lookupRow = createLookupRow(primaryKeyValues, primaryKeyIndexes);
        rowKeyExtractor.setRecord(lookupRow);
        BinaryRow key = rowKeyExtractor.trimmedPrimaryKey();
        BinaryRow partition = createPartition(partitionValues);
        LOG.info(
                "Calling Paimon LocalTableQuery lookup for partition {}, bucket {}, primary key indexes {}, primary key values {}.",
                partitionValues,
                bucket,
                Arrays.toString(primaryKeyIndexes),
                Arrays.toString(primaryKeyValues));
        long startMs = System.currentTimeMillis();
        org.apache.paimon.data.InternalRow row = tableQuery.lookup(partition, bucket, key);
        LOG.info(
                "Finished Paimon LocalTableQuery lookup for partition {}, bucket {}, hit {}, duration {} ms.",
                partitionValues,
                bucket,
                row != null,
                System.currentTimeMillis() - startMs);
        if (row != null) {
            return new PaimonRowAsFlussRow(row, !projected);
        }
        return null;
    }

    private BinaryRow createPartition(List<String> partitionValues) {
        if (partitionValues.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }
        GenericRow partitionRow = new GenericRow(partitionValues.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            partitionRow.setField(i, BinaryString.fromString(partitionValues.get(i)));
        }
        return partitionSerializer.toBinaryRow(partitionRow).copy();
    }

    private GenericRow createLookupRow(Object[] primaryKeyValues, int[] primaryKeyIndexes) {
        GenericRow lookupRow = new GenericRow(paimonRowType.getFieldCount());
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            int fieldIndex = primaryKeyIndexes[i];
            lookupRow.setField(
                    fieldIndex,
                    toPaimonLiteral(paimonRowType.getTypeAt(fieldIndex), primaryKeyValues[i]));
        }
        return lookupRow;
    }

    private PaimonPartitionBucket createPartitionBucket(PaimonSplit split) {
        return new PaimonPartitionBucket(split.dataSplit().partition(), split.dataSplit().bucket());
    }

    private void refreshFiles(
            PaimonPartitionBucket partitionBucket, Map<String, DataFileMeta> plannedFiles) {
        Map<String, DataFileMeta> knownFiles =
                loadedFiles.computeIfAbsent(partitionBucket, ignored -> new HashMap<>());

        List<DataFileMeta> beforeFiles =
                knownFiles.entrySet().stream()
                        .filter(entry -> !plannedFiles.containsKey(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        List<DataFileMeta> afterFiles = new ArrayList<>();
        for (Map.Entry<String, DataFileMeta> entry : plannedFiles.entrySet()) {
            if (!knownFiles.containsKey(entry.getKey())) {
                afterFiles.add(entry.getValue());
            }
        }
        for (DataFileMeta file : beforeFiles) {
            knownFiles.remove(file.fileName());
        }
        for (DataFileMeta file : afterFiles) {
            knownFiles.put(file.fileName(), file);
        }
        if (!beforeFiles.isEmpty() || !afterFiles.isEmpty()) {
            LOG.info(
                    "Calling Paimon LocalTableQuery refreshFiles for partition {}, bucket {}, before file count {}, after file count {}.",
                    partitionBucket.getPartition(),
                    partitionBucket.getBucket(),
                    beforeFiles.size(),
                    afterFiles.size());
            long startMs = System.currentTimeMillis();
            tableQuery.refreshFiles(
                    partitionBucket.getPartition(),
                    partitionBucket.getBucket(),
                    beforeFiles,
                    afterFiles);
            LOG.info(
                    "Finished Paimon LocalTableQuery refreshFiles for partition {}, bucket {}, before file count {}, after file count {}, duration {} ms.",
                    partitionBucket.getPartition(),
                    partitionBucket.getBucket(),
                    beforeFiles.size(),
                    afterFiles.size(),
                    System.currentTimeMillis() - startMs);
        }
    }

    private static IOManager createIOManager(@Nullable String ioManagerTmpDir) {
        String rootDir =
                ioManagerTmpDir == null ? System.getProperty("java.io.tmpdir") : ioManagerTmpDir;
        File tempDir = new File(rootDir, "fluss-paimon-lookup-" + UUID.randomUUID());
        LOG.info("Creating Paimon lookup IO manager under {}.", tempDir.getAbsolutePath());
        return IOManager.create(tempDir.getAbsolutePath());
    }

    @Override
    public synchronized void close() throws IOException {
        IOException exception = null;
        try {
            tableQuery.close();
        } catch (IOException e) {
            exception = e;
        }
        try {
            ioManager.close();
        } catch (Exception e) {
            if (exception == null) {
                exception = new IOException("Failed to close Paimon lookup IO manager.", e);
            } else {
                exception.addSuppressed(e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
