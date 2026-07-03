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
        if (fileStoreTable.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Paimon lake lookup only supports primary-key tables.");
        }
        this.ioManager = createIOManager();
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
        for (PaimonSplit split : splits) {
            refreshSplit(split);
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
        org.apache.paimon.data.InternalRow row = tableQuery.lookup(partition, bucket, key);
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

    private void refreshSplit(PaimonSplit split) {
        PaimonPartitionBucket partitionBucket =
                new PaimonPartitionBucket(
                        split.dataSplit().partition(), split.dataSplit().bucket());
        Map<String, DataFileMeta> knownFiles =
                loadedFiles.computeIfAbsent(partitionBucket, ignored -> new HashMap<>());

        List<DataFileMeta> beforeFiles =
                split.dataSplit().beforeFiles().stream()
                        .filter(file -> knownFiles.remove(file.fileName()) != null)
                        .collect(Collectors.toList());
        List<DataFileMeta> afterFiles = new ArrayList<>();
        for (DataFileMeta file : split.dataSplit().dataFiles()) {
            if (!knownFiles.containsKey(file.fileName())) {
                knownFiles.put(file.fileName(), file);
                afterFiles.add(file);
            }
        }
        if (!beforeFiles.isEmpty() || !afterFiles.isEmpty()) {
            LOG.info(
                    "Calling Paimon LocalTableQuery refreshFiles for partition {}, bucket {}, before file count {}, after file count {}.",
                    split.partition(),
                    split.bucket(),
                    beforeFiles.size(),
                    afterFiles.size());
            tableQuery.refreshFiles(
                    split.dataSplit().partition(),
                    split.dataSplit().bucket(),
                    beforeFiles,
                    afterFiles);
        }
    }

    private static IOManager createIOManager() {
        File tempDir =
                new File(
                        System.getProperty("java.io.tmpdir"),
                        "fluss-paimon-lookup-" + UUID.randomUUID());
        return IOManager.create(tempDir.getAbsolutePath());
    }

    @Override
    public void close() throws IOException {
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
