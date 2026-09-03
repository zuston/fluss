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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Looks up a primary key by scanning the latest Paimon snapshot with a limit of one result.
 *
 * <p>Each request has its own scan and reader, restricted to the requested partition, bucket, and
 * complete primary key. No local lookup files or partition-bucket file lists are cached. Close is
 * expected only after the owner has drained active lookups.
 */
public class PaimonScanLakeTableLookuper implements LakeTableLookuper {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final TableConfig tableConfig;
    private final Object initializationLock = new Object();

    private @Nullable Catalog catalog;
    private @Nullable PaimonLookupRowConverter rowConverter;
    private volatile @Nullable FileStoreTable fileStoreTable;
    private volatile boolean closed;

    public PaimonScanLakeTableLookuper(
            Configuration paimonConfig, TablePath tablePath, TableConfig tableConfig) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
    }

    @Override
    public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        checkNotClosed();
        ensureInitialized(context.valueRowType());

        long lookupStartNanos = System.nanoTime();
        try {
            ReadBuilder readBuilder =
                    fileStoreTable
                            .newReadBuilder()
                            .withFilter(keyPredicates(rowConverter.getKey(key, context)))
                            .withProjection(rowConverter.valueProjection())
                            .withLimit(1);
            InnerTableScan scan =
                    ((InnerTableScan) readBuilder.newScan())
                            .withPartitionFilter(
                                    Collections.singletonList(rowConverter.getPartition(context)))
                            .withBucket(context.bucketId());

            // Filter pushdown alone is not exact. Apply the predicate to merged rows before
            // taking the first result, and close the reader immediately to enforce limit 1.
            try (CloseableIterator<InternalRow> rows =
                    readBuilder
                            .newRead()
                            .executeFilter()
                            .createReader(scan.plan())
                            .toCloseableIterator()) {
                return rows.hasNext() ? rowConverter.encodeValue(rows.next(), context) : null;
            }
        } catch (Exception e) {
            // Paimon's iterator may wrap I/O errors in RuntimeException. Preserve the existing
            // retriable KV error contract, including compaction/expiration races after planning.
            if (ExceptionUtils.findThrowable(e, IOException.class).isPresent()) {
                throw new KvStorageException(
                        "Failed to scan historical data from Paimon for " + tablePath + ".", e);
            }
            throw e;
        } finally {
            context.lookupMetricRecorder()
                    .recordLookup(System.nanoTime() - lookupStartNanos, false);
        }
    }

    @Override
    public void requestRefresh() {
        checkNotClosed();
        // Every lookup plans a new scan against the latest snapshot; no registered file set exists.
    }

    private List<Predicate> keyPredicates(BinaryRow key) {
        org.apache.paimon.types.RowType rowType = fileStoreTable.rowType();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> keys = fileStoreTable.schema().trimmedPrimaryKeys();
        List<Predicate> predicates = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            int fieldIndex = rowType.getFieldIndex(keys.get(i));
            Object value =
                    InternalRow.createFieldGetter(rowType.getTypeAt(fieldIndex), i)
                            .getFieldOrNull(key);
            predicates.add(
                    value == null ? builder.isNull(fieldIndex) : builder.equal(fieldIndex, value));
        }
        return predicates;
    }

    private void ensureInitialized(RowType valueRowType) throws Exception {
        if (fileStoreTable == null) {
            synchronized (initializationLock) {
                checkNotClosed();
                if (fileStoreTable == null) {
                    Catalog newCatalog =
                            CatalogFactory.createCatalog(
                                    CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
                    boolean initialized = false;
                    try {
                        FileStoreTable newTable =
                                (FileStoreTable) newCatalog.getTable(toPaimon(tablePath));
                        PaimonLookupRowConverter newRowConverter =
                                new PaimonLookupRowConverter(
                                        tableConfig, newTable.schema(), valueRowType);
                        catalog = newCatalog;
                        rowConverter = newRowConverter;
                        // Publish all initialized fields together.
                        fileStoreTable = newTable;
                        initialized = true;
                    } finally {
                        if (!initialized) {
                            IOUtils.closeQuietly(newCatalog, "Paimon catalog");
                        }
                    }
                }
            }
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Paimon scan lake table lookuper has been closed.");
        }
    }

    @Override
    public void close() {
        synchronized (initializationLock) {
            if (closed) {
                return;
            }
            closed = true;
            IOUtils.closeQuietly(catalog, "Paimon catalog");
            fileStoreTable = null;
            rowConverter = null;
            catalog = null;
        }
    }
}
