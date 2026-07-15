/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.utils.FlussToPaimonPredicateConverter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SegmentsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.MapUtils.newConcurrentHashMap;

/**
 * Paimon Lake format implementation of {@link org.apache.fluss.lake.source.LakeSource} for reading
 * paimon table.
 */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeSource.class);

    private static final String METADATA_CACHE_ENABLED_KEY = "fluss.paimon.metadata-cache.enabled";
    private static final String METADATA_CACHE_MAX_MEMORY_KEY =
            "fluss.paimon.metadata-cache.max-memory-size";
    private static final String METADATA_CACHE_MAX_FILE_KEY =
            "fluss.paimon.metadata-cache.max-file-size";
    private static final String METADATA_CACHE_SNAPSHOT_MAX_ENTRIES_KEY =
            "fluss.paimon.metadata-cache.snapshot-max-entries";
    private static final String DEFAULT_METADATA_CACHE_MAX_MEMORY = "64mb";
    private static final String DEFAULT_METADATA_CACHE_MAX_FILE = "16mb";
    private static final int DEFAULT_METADATA_CACHE_SNAPSHOT_MAX_ENTRIES = 1024;

    private static final ConcurrentMap<TableCacheKey, FileStoreTable> TABLE_CACHE =
            newConcurrentHashMap();
    private static final ConcurrentMap<TableCacheKey, SegmentsCache<Path>> MANIFEST_CACHE =
            newConcurrentHashMap();
    private static final ConcurrentMap<TableCacheKey, Cache<Path, Snapshot>> SNAPSHOT_CACHE =
            newConcurrentHashMap();

    private final Configuration paimonConfig;
    private final TableCacheKey tableCacheKey;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tableCacheKey = new TableCacheKey(paimonConfig.toMap(), tablePath);
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<org.apache.paimon.predicate.Predicate> converted = new ArrayList<>();
        RowType rowType = getRowType();
        for (Predicate predicate : predicates) {
            Optional<org.apache.paimon.predicate.Predicate> optPredicate =
                    FlussToPaimonPredicateConverter.convert(rowType, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        if (!converted.isEmpty()) {
            predicate = PredicateBuilder.and(converted);
        }
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<PaimonSplit> createPlanner(PlannerContext plannerContext) {
        return new PaimonSplitPlanner(getTable(), predicate, plannerContext.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<PaimonSplit> context) throws IOException {
        try {
            FileStoreTable fileStoreTable = getTable();
            if (fileStoreTable.primaryKeys().isEmpty()) {
                return new PaimonRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            } else {
                return new PaimonSortedRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            }
        } catch (Exception e) {
            throw new IOException("Fail to create record reader.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<PaimonSplit> getSplitSerializer() {
        return new PaimonSplitSerializer();
    }

    private Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private FileStoreTable getTable() {
        return TABLE_CACHE.computeIfAbsent(tableCacheKey, ignored -> loadTable());
    }

    private FileStoreTable loadTable() {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            configureMetadataCache(fileStoreTable);
            return fileStoreTable;
        } catch (Exception e) {
            throw new RuntimeException("Fail to get table " + tablePath, e);
        }
    }

    private void configureMetadataCache(FileStoreTable fileStoreTable) {
        if (!Boolean.parseBoolean(
                getPaimonConfig(METADATA_CACHE_ENABLED_KEY, Boolean.TRUE.toString()))) {
            return;
        }

        MemorySize manifestCacheMaxMemory =
                getPaimonMemorySize(
                        METADATA_CACHE_MAX_MEMORY_KEY, DEFAULT_METADATA_CACHE_MAX_MEMORY);
        if (manifestCacheMaxMemory.getBytes() > 0) {
            SegmentsCache<Path> manifestCache =
                    MANIFEST_CACHE.computeIfAbsent(
                            tableCacheKey,
                            ignored -> {
                                MemorySize maxFileSize =
                                        getPaimonMemorySize(
                                                METADATA_CACHE_MAX_FILE_KEY,
                                                DEFAULT_METADATA_CACHE_MAX_FILE);
                                LOG.info(
                                        "Created Paimon manifest metadata cache for table {}, maxMemory {}, maxFileSize {}.",
                                        tablePath,
                                        manifestCacheMaxMemory,
                                        maxFileSize);
                                return SegmentsCache.create(
                                        manifestCacheMaxMemory, maxFileSize.getBytes());
                            });
            fileStoreTable.setManifestCache(manifestCache);
        }

        int snapshotMaxEntries =
                getPositiveInt(
                        METADATA_CACHE_SNAPSHOT_MAX_ENTRIES_KEY,
                        DEFAULT_METADATA_CACHE_SNAPSHOT_MAX_ENTRIES);
        if (snapshotMaxEntries > 0) {
            Cache<Path, Snapshot> snapshotCache =
                    SNAPSHOT_CACHE.computeIfAbsent(
                            tableCacheKey,
                            ignored -> {
                                LOG.info(
                                        "Created Paimon snapshot metadata cache for table {}, maxEntries {}.",
                                        tablePath,
                                        snapshotMaxEntries);
                                return Caffeine.newBuilder()
                                        .softValues()
                                        .maximumSize(snapshotMaxEntries)
                                        .executor(Runnable::run)
                                        .build();
                            });
            fileStoreTable.setSnapshotCache(snapshotCache);
        }
    }

    private MemorySize getPaimonMemorySize(String key, String defaultValue) {
        return MemorySize.parse(getPaimonConfig(key, defaultValue));
    }

    private int getPositiveInt(String key, int defaultValue) {
        String value = getPaimonConfig(key, String.valueOf(defaultValue));
        int parsed = Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Configuration '%s' must be non-negative, but was %s.", key, value));
        }
        return parsed;
    }

    private String getPaimonConfig(String key, String defaultValue) {
        return paimonConfig.toMap().getOrDefault(key, defaultValue);
    }

    private static FileStoreTable getTable(Catalog catalog, TablePath tablePath) throws Exception {
        return (FileStoreTable) catalog.getTable(toPaimon(tablePath));
    }

    private RowType getRowType() {
        return getTable().rowType();
    }

    private static class TableCacheKey {
        private final Map<String, String> paimonConfig;
        private final TablePath tablePath;

        private TableCacheKey(Map<String, String> paimonConfig, TablePath tablePath) {
            this.paimonConfig = Collections.unmodifiableMap(new HashMap<>(paimonConfig));
            this.tablePath = tablePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableCacheKey)) {
                return false;
            }
            TableCacheKey that = (TableCacheKey) o;
            return Objects.equals(paimonConfig, that.paimonConfig)
                    && Objects.equals(tablePath, that.tablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(paimonConfig, tablePath);
        }
    }
}
