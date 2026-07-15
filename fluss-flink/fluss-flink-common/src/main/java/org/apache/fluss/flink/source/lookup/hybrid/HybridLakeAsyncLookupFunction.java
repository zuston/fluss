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

package org.apache.fluss.flink.source.lookup.hybrid;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.source.lookup.FlussAsyncLookupClient;
import org.apache.fluss.flink.source.lookup.LookupNormalizer;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An async lookup function that falls back to lake data when cold Fluss lookup misses. */
public class HybridLakeAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLakeAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;
    private static final long READABLE_LAKE_SNAPSHOT_CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long PARTITION_EXISTS_CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(1);

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final int[] primaryKeyIndexes;
    private final int[] partitionKeyIndexes;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;
    private final Map<String, String> tableOptions;
    @Nullable private final String autoPartitionKey;
    private final Duration lakeFallbackTimeout;
    private final int lakeFallbackExecutorThreads;
    private final int lakeFallbackMaxConcurrency;

    private transient FlussAsyncLookupClient flussLookupClient;
    private transient Admin admin;
    private transient InternalRow.FieldGetter[] primaryKeyFieldGetters;
    private transient org.apache.fluss.types.RowType flussFullRowType;
    private transient ThreadPoolExecutor lakeLookupExecutor;
    private transient ScheduledExecutorService timeoutExecutor;
    private transient AtomicInteger lakeFallbackPendingCount;
    private transient HybridLookupMetrics metrics;
    private transient Object readableLakeSnapshotCacheLock;
    @Nullable private transient CachedLakeSnapshot readableLakeSnapshotCache;
    private transient Object partitionExistsCacheLock;
    private transient Map<PartitionSpec, CachedPartitionExistence> partitionExistsCache;
    private transient volatile boolean closed;

    public HybridLakeAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] primaryKeyIndexes,
            int[] partitionKeyIndexes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            Map<String, String> tableOptions,
            Duration lakeFallbackTimeout,
            int lakeFallbackExecutorThreads,
            int lakeFallbackMaxConcurrency) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.tableOptions = tableOptions;
        Configuration tableConfig = Configuration.fromMap(tableOptions);
        this.autoPartitionKey = tableConfig.getString(ConfigOptions.TABLE_AUTO_PARTITION_KEY);
        this.lakeFallbackTimeout = lakeFallbackTimeout;
        this.lakeFallbackExecutorThreads = lakeFallbackExecutorThreads;
        this.lakeFallbackMaxConcurrency = lakeFallbackMaxConcurrency;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("Start opening hybrid lake async lookup function for table {}.", tablePath);
        closed = false;
        flussFullRowType = FlinkConversions.toFlussRowType(flinkRowType);
        validateLookupShape();
        flussLookupClient =
                new FlussAsyncLookupClient(
                        flussConfig, tablePath, flinkRowType, lookupNormalizer, projection, false);
        flussLookupClient.open();
        projection = flussLookupClient.projection();
        admin = flussLookupClient.connection().getAdmin();

        org.apache.fluss.types.RowType primaryKeyRowType =
                flussFullRowType.project(primaryKeyIndexes);
        primaryKeyFieldGetters = new InternalRow.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyFieldGetters[i] =
                    InternalRow.createFieldGetter(primaryKeyRowType.getTypeAt(i), i);
        }
        validatePartitionKeysInPrimaryKey();
        validateAutoPartitionKeyInPrimaryKey();

        lakeLookupExecutor =
                new ThreadPoolExecutor(
                        lakeFallbackExecutorThreads,
                        lakeFallbackExecutorThreads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(lakeFallbackMaxConcurrency),
                        new ExecutorThreadFactory("fluss-lake-fallback-lookup"),
                        new ThreadPoolExecutor.AbortPolicy());
        timeoutExecutor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-lake-fallback-timeout"));
        lakeFallbackPendingCount = new AtomicInteger();
        readableLakeSnapshotCacheLock = new Object();
        partitionExistsCacheLock = new Object();
        partitionExistsCache = new HashMap<>();
        metrics =
                new HybridLookupMetrics(
                        tablePath,
                        lakeLookupExecutor,
                        lakeFallbackPendingCount,
                        lakeFallbackTimeout);
        metrics.open(context);
        LOG.info("Finished opening hybrid lake async lookup function for table {}.", tablePath);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        FlussAsyncLookupClient.LookupRequest lookupRequest =
                flussLookupClient.prepareLookup(keyRow);
        FlussLookupKey lookupKey = createLookupKey(lookupRequest.normalizedKeyRow());

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        if (closed) {
            future.complete(Collections.emptyList());
            return future;
        }
        long flussLookupStartMs = System.currentTimeMillis();
        flussLookupClient
                .lookup(lookupRequest.flussKeyRow())
                .whenComplete(
                        (result, throwable) -> {
                            if (closed) {
                                future.complete(Collections.emptyList());
                            } else if (throwable != null) {
                                metrics.recordFlussFailure(elapsedMillis(flussLookupStartMs));
                                if (shouldFallbackToLakeOnFlussFailure(throwable)) {
                                    fallbackToLakeIfPartitionMissing(
                                            lookupKey,
                                            lookupRequest.remainingFilter(),
                                            future,
                                            throwable);
                                    return;
                                }
                                LOG.error(
                                        "Fluss async lookup failed for table {}.",
                                        tablePath,
                                        throwable);
                                future.completeExceptionally(
                                        new RuntimeException(
                                                "Execution of Fluss async lookup failed: "
                                                        + throwable.getMessage(),
                                                throwable));
                            } else {
                                boolean flussHit = !result.getRowList().isEmpty();
                                metrics.recordFlussCompletion(
                                        elapsedMillis(flussLookupStartMs), flussHit);
                                if (!result.getRowList().isEmpty()) {
                                    metrics.incHotFlussHits();
                                    handleLookupSuccess(
                                            future, result, lookupRequest.remainingFilter());
                                } else {
                                    fallbackToLakeIfPartitionMissing(
                                            lookupKey, lookupRequest.remainingFilter(), future);
                                }
                            }
                        });
        return future;
    }

    private boolean shouldFallbackToLakeOnFlussFailure(Throwable throwable) {
        return ExceptionUtils.findThrowable(throwable, InvalidMetadataException.class).isPresent();
    }

    private void fallbackToLakeIfPartitionMissing(
            FlussLookupKey lookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        fallbackToLakeIfPartitionMissing(lookupKey, remainingFilter, future, null);
    }

    private void fallbackToLakeIfPartitionMissing(
            FlussLookupKey lookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future,
            @Nullable Throwable flussLookupFailure) {
        partitionExistsAsync(lookupKey, flussLookupFailure != null)
                .whenComplete(
                        (exists, throwable) -> {
                            if (closed) {
                                future.complete(Collections.emptyList());
                            } else if (throwable != null) {
                                LOG.warn(
                                        "Cannot determine whether Fluss lookup for table {}, partition {} can fall back to lake lookup by partition metadata.",
                                        tablePath,
                                        lookupKey.partitionValue,
                                        throwable);
                                if (flussLookupFailure != null) {
                                    completeFlussLookupExceptionally(future, flussLookupFailure);
                                } else {
                                    future.completeExceptionally(throwable);
                                }
                            } else if (exists) {
                                metrics.incHotFlussMisses();
                                if (flussLookupFailure != null) {
                                    completeFlussLookupExceptionally(future, flussLookupFailure);
                                } else {
                                    future.complete(Collections.emptyList());
                                }
                            } else {
                                metrics.incColdFlussMisses();
                                if (flussLookupFailure != null) {
                                    LOG.warn(
                                            "Fluss async lookup failed with invalid metadata for a lake-only partition in table {}, partition {}, primary key indexes {}, primary key values {}. Falling back to lake lookup.",
                                            tablePath,
                                            lookupKey.partitionValue,
                                            Arrays.toString(primaryKeyIndexes),
                                            Arrays.toString(lookupKey.primaryKeyValues),
                                            flussLookupFailure);
                                }
                                lookupLakeAsync(lookupKey, remainingFilter, future);
                            }
                        });
    }

    private void completeFlussLookupExceptionally(
            CompletableFuture<Collection<RowData>> future, Throwable throwable) {
        LOG.error("Fluss async lookup failed for table {}.", tablePath, throwable);
        future.completeExceptionally(
                new RuntimeException(
                        "Execution of Fluss async lookup failed: " + throwable.getMessage(),
                        throwable));
    }

    private void lookupLakeAsync(
            FlussLookupKey lookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        if (closed) {
            future.complete(Collections.emptyList());
            return;
        }
        metrics.recordLakeRequest();
        lakeFallbackPendingCount.incrementAndGet();
        long startMs = System.currentTimeMillis();
        scheduleTimeout(future, lookupKey, startMs);
        try {
            lakeLookupExecutor.execute(
                    () -> {
                        metrics.recordLakeStageLatency(
                                HybridLookupMetrics.LakeLookupStage.QUEUE, elapsedMillis(startMs));
                        try {
                            Collection<RowData> rows =
                                    closed
                                            ? Collections.emptyList()
                                            : lookupLake(lookupKey, remainingFilter);
                            completeLakeFallbackSuccessfully(future, rows, startMs);
                        } catch (Throwable t) {
                            completeLakeFallbackExceptionally(
                                    future,
                                    new RuntimeException(
                                            "Execution of lake fallback lookup failed: "
                                                    + t.getMessage(),
                                            t),
                                    HybridLookupMetrics.LakeFallbackOutcome.FAILURE,
                                    startMs);
                        }
                    });
        } catch (RuntimeException e) {
            completeLakeFallbackExceptionally(
                    future,
                    new RuntimeException("Lake fallback lookup executor is overloaded.", e),
                    HybridLookupMetrics.LakeFallbackOutcome.REJECTED,
                    startMs);
        }
    }

    private void scheduleTimeout(
            CompletableFuture<Collection<RowData>> future, FlussLookupKey lookupKey, long startMs) {
        timeoutExecutor.schedule(
                () ->
                        completeLakeFallbackOnTimeout(
                                future,
                                lookupKey,
                                new TimeoutException(
                                        "Lake fallback lookup timed out after "
                                                + lakeFallbackTimeout),
                                startMs),
                lakeFallbackTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void completeLakeFallbackSuccessfully(
            CompletableFuture<Collection<RowData>> future, Collection<RowData> rows, long startMs) {
        if (future.complete(rows)) {
            long latencyMs = elapsedMillis(startMs);
            metrics.recordLakeCompletion(latencyMs, !rows.isEmpty());
            lakeFallbackPendingCount.decrementAndGet();
        }
    }

    private void completeLakeFallbackExceptionally(
            CompletableFuture<Collection<RowData>> future,
            Throwable throwable,
            HybridLookupMetrics.LakeFallbackOutcome outcome,
            long startMs) {
        if (future.completeExceptionally(throwable)) {
            long latencyMs = elapsedMillis(startMs);
            metrics.recordLakeFailure(latencyMs, outcome);
            lakeFallbackPendingCount.decrementAndGet();
        }
    }

    private void completeLakeFallbackOnTimeout(
            CompletableFuture<Collection<RowData>> future,
            FlussLookupKey lookupKey,
            TimeoutException timeoutException,
            long startMs) {
        if (future.complete(Collections.emptyList())) {
            long latencyMs = elapsedMillis(startMs);
            metrics.recordLakeFailure(latencyMs, HybridLookupMetrics.LakeFallbackOutcome.TIMEOUT);
            lakeFallbackPendingCount.decrementAndGet();
            LOG.warn(
                    "Lake fallback lookup timed out for table {}, partition {}, primary key indexes {}, primary key values {}, timeout {}, latencyMs {}. Completing with empty result to avoid failing the lookup.",
                    tablePath,
                    lookupKey.partitionValue,
                    Arrays.toString(primaryKeyIndexes),
                    Arrays.toString(lookupKey.primaryKeyValues),
                    lakeFallbackTimeout,
                    latencyMs,
                    timeoutException);
        }
    }

    private Collection<RowData> lookupLake(
            FlussLookupKey lookupKey, @Nullable LookupNormalizer.RemainingFilter remainingFilter)
            throws Exception {
        if (closed) {
            return Collections.emptyList();
        }
        LakeSnapshot lakeSnapshot;
        long stageStartMs = System.currentTimeMillis();
        try {
            lakeSnapshot = getCachedReadableLakeSnapshot();
        } catch (Exception e) {
            Throwable stripped = ExceptionUtils.stripExecutionException(e);
            if (stripped instanceof LakeTableSnapshotNotExistException) {
                return Collections.emptyList();
            }
            throw e;
        } finally {
            metrics.recordLakeStageLatency(
                    HybridLookupMetrics.LakeLookupStage.SNAPSHOT, elapsedMillis(stageStartMs));
        }

        stageStartMs = System.currentTimeMillis();
        LakeSource<LakeSplit> lakeSource;
        try {
            lakeSource =
                    checkNotNull(
                            createLakeSource(tablePath, tableOptions),
                            "Lake source must not be null for lake fallback lookup.");
            if (projection != null) {
                lakeSource.withProject(toNestedProjection(projection));
            }
            Predicate predicate = createPrimaryKeyPredicate(lookupKey.primaryKeyValues);
            LakeSource.FilterPushDownResult pushDownResult =
                    lakeSource.withFilters(Collections.singletonList(predicate));
            if (!pushDownResult.remainingPredicates().isEmpty()) {
                throw new TableException(
                        "Lake fallback lookup requires primary-key predicates to be pushed down.");
            }
        } finally {
            metrics.recordLakeStageLatency(
                    HybridLookupMetrics.LakeLookupStage.SOURCE_FILTER, elapsedMillis(stageStartMs));
        }

        stageStartMs = System.currentTimeMillis();
        List<LakeSplit> splits;
        try {
            Planner<LakeSplit> planner = lakeSource.createPlanner(lakeSnapshot::getSnapshotId);
            LOG.debug(
                    "Planning lake lookup splits for table {}, snapshot {}, primary key indexes {}, primary key values {}.",
                    tablePath,
                    lakeSnapshot.getSnapshotId(),
                    Arrays.toString(primaryKeyIndexes),
                    Arrays.toString(lookupKey.primaryKeyValues));
            splits = planner.plan();
        } finally {
            metrics.recordLakeStageLatency(
                    HybridLookupMetrics.LakeLookupStage.PLAN, elapsedMillis(stageStartMs));
        }

        if (closed) {
            return Collections.emptyList();
        }
        long splitLookupStartMs = System.currentTimeMillis();
        boolean splitLookupRecorded = false;
        HybridLookupMetrics.LakeLookupFileStats fileStats =
                new HybridLookupMetrics.LakeLookupFileStats(splits.size());
        try {
            for (LakeSplit split : splits) {
                if (!matchesLookupPartition(split, lookupKey.partitionValue)) {
                    continue;
                }
                fileStats.recordMatchedSplit(split);
                LOG.debug(
                        "Reading lake split for table {}, snapshot {}, partition {}, bucket {}, primary key indexes {}, primary key values {}.",
                        tablePath,
                        lakeSnapshot.getSnapshotId(),
                        split.partition(),
                        split.bucket(),
                        Arrays.toString(primaryKeyIndexes),
                        Arrays.toString(lookupKey.primaryKeyValues));
                RecordReader reader = lakeSource.createRecordReader(() -> split);
                try (CloseableIterator<LogRecord> iterator = reader.read()) {
                    while (iterator.hasNext()) {
                        metrics.recordLakeStageLatency(
                                HybridLookupMetrics.LakeLookupStage.SPLIT_LOOKUP,
                                elapsedMillis(splitLookupStartMs));
                        splitLookupRecorded = true;

                        stageStartMs = System.currentTimeMillis();
                        try {
                            RowData flinkRow =
                                    flussLookupClient
                                            .rowConverter()
                                            .toFlinkRowData(iterator.next().getRow());
                            if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                                return Collections.singletonList(flinkRow);
                            }
                        } finally {
                            metrics.recordLakeStageLatency(
                                    HybridLookupMetrics.LakeLookupStage.ROW_CONVERT_FILTER,
                                    elapsedMillis(stageStartMs));
                        }
                        splitLookupStartMs = System.currentTimeMillis();
                        splitLookupRecorded = false;
                    }
                }
            }
        } finally {
            metrics.recordLakeFileStats(fileStats);
            if (!splitLookupRecorded) {
                metrics.recordLakeStageLatency(
                        HybridLookupMetrics.LakeLookupStage.SPLIT_LOOKUP,
                        elapsedMillis(splitLookupStartMs));
            }
        }
        return Collections.emptyList();
    }

    private boolean matchesLookupPartition(LakeSplit split, String partitionValue) {
        return split.partition().isEmpty() || split.partition().contains(partitionValue);
    }

    private CompletableFuture<Boolean> partitionExistsAsync(
            FlussLookupKey lookupKey, boolean refresh) {
        long nowMs = System.currentTimeMillis();
        CachedPartitionExistence cached = partitionExistsCache.get(lookupKey.partitionSpec);
        if (!refresh && cached != null && cached.isValid(nowMs)) {
            return CompletableFuture.completedFuture(cached.exists);
        }

        synchronized (partitionExistsCacheLock) {
            nowMs = System.currentTimeMillis();
            cached = partitionExistsCache.get(lookupKey.partitionSpec);
            if (!refresh && cached != null && cached.isValid(nowMs)) {
                return CompletableFuture.completedFuture(cached.exists);
            }
        }

        return admin.listPartitionInfos(tablePath, lookupKey.partitionSpec)
                .thenApply(partitions -> partitionExistsInMetadata(lookupKey, partitions))
                .whenComplete(
                        (exists, throwable) -> {
                            if (throwable == null) {
                                cachePartitionExistence(lookupKey.partitionSpec, exists);
                            }
                        });
    }

    private boolean partitionExistsInMetadata(
            FlussLookupKey lookupKey, List<PartitionInfo> partitions) {
        for (PartitionInfo partition : partitions) {
            if (partition.getPartitionName().equals(lookupKey.partitionValue)) {
                return true;
            }
        }
        LOG.debug(
                "Partition {} no longer exists in Fluss metadata for table {}. Lake fallback is allowed.",
                lookupKey.partitionValue,
                tablePath);
        return false;
    }

    private void cachePartitionExistence(PartitionSpec partitionSpec, boolean exists) {
        long nowMs = System.currentTimeMillis();
        synchronized (partitionExistsCacheLock) {
            partitionExistsCache.put(
                    partitionSpec,
                    new CachedPartitionExistence(exists, nowMs + PARTITION_EXISTS_CACHE_TTL_MS));
        }
    }

    private LakeSnapshot getCachedReadableLakeSnapshot() throws Exception {
        long nowMs = System.currentTimeMillis();
        CachedLakeSnapshot cached = readableLakeSnapshotCache;
        if (cached != null && cached.isValid(nowMs)) {
            return cached.snapshot;
        }

        synchronized (readableLakeSnapshotCacheLock) {
            nowMs = System.currentTimeMillis();
            cached = readableLakeSnapshotCache;
            if (cached != null && cached.isValid(nowMs)) {
                return cached.snapshot;
            }

            LakeSnapshot lakeSnapshot = getReadableLakeSnapshotWithRetry();
            readableLakeSnapshotCache =
                    new CachedLakeSnapshot(
                            lakeSnapshot, nowMs + READABLE_LAKE_SNAPSHOT_CACHE_TTL_MS);
            LOG.debug(
                    "Refreshed readable lake snapshot cache for table {}, snapshot {}, ttlMs {}.",
                    tablePath,
                    lakeSnapshot.getSnapshotId(),
                    READABLE_LAKE_SNAPSHOT_CACHE_TTL_MS);
            return lakeSnapshot;
        }
    }

    private LakeSnapshot getReadableLakeSnapshotWithRetry() throws Exception {
        try {
            return getReadableLakeSnapshot();
        } catch (Exception firstException) {
            if (!isMissingLakeSnapshotOffsetsFile(firstException)) {
                throw firstException;
            }
            LOG.warn(
                    "Failed to read readable lake snapshot offsets for table {}. Retrying once to reload latest readable snapshot from ZooKeeper.",
                    tablePath,
                    firstException);
            try {
                return getReadableLakeSnapshot();
            } catch (Exception retryException) {
                retryException.addSuppressed(firstException);
                throw retryException;
            }
        }
    }

    private LakeSnapshot getReadableLakeSnapshot() throws Exception {
        return admin.getReadableLakeSnapshot(tablePath).get();
    }

    private boolean isMissingLakeSnapshotOffsetsFile(Throwable throwable) {
        Throwable stripped = ExceptionUtils.stripExecutionException(throwable);
        if (ExceptionUtils.findThrowable(stripped, FileNotFoundException.class).isPresent()) {
            return true;
        }
        return ExceptionUtils.findThrowable(
                        stripped,
                        cause -> {
                            String message = cause.getMessage();
                            return message != null
                                    && message.contains(FileNotFoundException.class.getName())
                                    && message.contains(".offsets");
                        })
                .isPresent();
    }

    private Predicate createPrimaryKeyPredicate(Object[] primaryKeyValues) {
        PredicateBuilder builder = new PredicateBuilder(flussFullRowType);
        List<Predicate> predicates = new ArrayList<>();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            predicates.add(builder.equal(primaryKeyIndexes[i], primaryKeyValues[i]));
        }
        return PredicateBuilder.and(predicates);
    }

    private void handleLookupSuccess(
            CompletableFuture<Collection<RowData>> resultFuture,
            LookupResult lookupResult,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter) {
        if (lookupResult.getRowList().isEmpty()) {
            resultFuture.complete(Collections.emptyList());
            return;
        }
        resultFuture.complete(flussLookupClient.toFlinkRows(lookupResult, remainingFilter));
    }

    private FlussLookupKey createLookupKey(RowData normalizedKeyRow) {
        InternalRow row = new FlinkAsFlussRow(normalizedKeyRow);
        Object[] primaryKeyValues = new Object[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyValues[i] = primaryKeyFieldGetters[i].getFieldOrNull(row);
        }
        PartitionSpec partitionSpec = createPartitionSpec(primaryKeyValues);
        String partitionName =
                ResolvedPartitionSpec.fromPartitionSpec(getPartitionFieldNames(), partitionSpec)
                        .getPartitionName();
        return new FlussLookupKey(primaryKeyValues, partitionSpec, partitionName);
    }

    private PartitionSpec createPartitionSpec(Object[] primaryKeyValues) {
        Map<String, String> partitionSpec = new HashMap<>();
        List<String> fieldNames = flinkRowType.getFieldNames();
        for (int partitionKeyIndex : partitionKeyIndexes) {
            int partitionKeyPositionInPrimaryKey =
                    findPrimaryKeyPosition(partitionKeyIndex, "partition key");
            Object partitionValue = primaryKeyValues[partitionKeyPositionInPrimaryKey];
            partitionSpec.put(
                    fieldNames.get(partitionKeyIndex), partitionValueToString(partitionValue));
        }
        return new PartitionSpec(partitionSpec);
    }

    private List<String> getPartitionFieldNames() {
        List<String> fieldNames = flinkRowType.getFieldNames();
        List<String> partitionFieldNames = new ArrayList<>(partitionKeyIndexes.length);
        for (int partitionKeyIndex : partitionKeyIndexes) {
            partitionFieldNames.add(fieldNames.get(partitionKeyIndex));
        }
        return partitionFieldNames;
    }

    private static String partitionValueToString(Object partitionValue) {
        if (partitionValue instanceof BinaryString) {
            return partitionValue.toString();
        }
        return String.valueOf(partitionValue);
    }

    private void validateAutoPartitionKeyInPrimaryKey() {
        int partitionKeyIndex = findAutoPartitionKeyIndex();
        findPrimaryKeyPosition(partitionKeyIndex, "auto partition key");
    }

    private void validatePartitionKeysInPrimaryKey() {
        for (int partitionKeyIndex : partitionKeyIndexes) {
            findPrimaryKeyPosition(partitionKeyIndex, "partition key");
        }
    }

    private int findPrimaryKeyPosition(int fieldIndex, String fieldRole) {
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            if (primaryKeyIndexes[i] == fieldIndex) {
                return i;
            }
        }
        throw new TableException(
                "Lake fallback lookup requires " + fieldRole + " to be part of primary key.");
    }

    private int findAutoPartitionKeyIndex() {
        if (autoPartitionKey == null) {
            return partitionKeyIndexes[0];
        }

        List<String> fieldNames = flinkRowType.getFieldNames();
        for (int partitionKeyIndex : partitionKeyIndexes) {
            if (fieldNames.get(partitionKeyIndex).equals(autoPartitionKey)) {
                return partitionKeyIndex;
            }
        }
        throw new TableException(
                "Lake fallback lookup requires auto partition key '"
                        + autoPartitionKey
                        + "' to be one of the partition keys.");
    }

    private void validateLookupShape() {
        if (partitionKeyIndexes.length == 0) {
            throw new TableException("Lake fallback lookup requires a partitioned table.");
        }
        if (lakeFallbackExecutorThreads <= 0 || lakeFallbackMaxConcurrency <= 0) {
            throw new TableException("Lake fallback lookup executor settings must be positive.");
        }
        if (lakeFallbackExecutorThreads > lakeFallbackMaxConcurrency) {
            throw new TableException(
                    "Option 'lookup.lake-fallback.executor-threads' must not exceed "
                            + "'lookup.lake-fallback.max-concurrency'.");
        }
        org.apache.fluss.types.DataType partitionType =
                flussFullRowType.getTypeAt(partitionKeyIndexes[0]);
        if (partitionType.getTypeRoot() != DataTypeRoot.STRING
                && partitionType.getTypeRoot() != DataTypeRoot.CHAR) {
            throw new TableException(
                    "Lake fallback lookup currently requires the partition key to be STRING/CHAR.");
        }
        if (!flinkRowType
                .getTypeAt(partitionKeyIndexes[0])
                .getTypeRoot()
                .getFamilies()
                .contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new TableException(
                    "Lake fallback lookup currently requires the partition key to be a character string.");
        }
    }

    private int[][] toNestedProjection(int[] projectedFields) {
        int[][] nestedProjection = new int[projectedFields.length][1];
        for (int i = 0; i < projectedFields.length; i++) {
            nestedProjection[i][0] = projectedFields[i];
        }
        return nestedProjection;
    }

    private static long elapsedMillis(long startMs) {
        return Math.max(0L, System.currentTimeMillis() - startMs);
    }

    private static class CachedLakeSnapshot {
        private final LakeSnapshot snapshot;
        private final long expireAtMs;

        private CachedLakeSnapshot(LakeSnapshot snapshot, long expireAtMs) {
            this.snapshot = snapshot;
            this.expireAtMs = expireAtMs;
        }

        private boolean isValid(long nowMs) {
            return nowMs < expireAtMs;
        }
    }

    private static class CachedPartitionExistence {
        private final boolean exists;
        private final long expireAtMs;

        private CachedPartitionExistence(boolean exists, long expireAtMs) {
            this.exists = exists;
            this.expireAtMs = expireAtMs;
        }

        private boolean isValid(long nowMs) {
            return nowMs < expireAtMs;
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing hybrid lake async lookup function for table {}.", tablePath);
        closed = true;
        if (metrics != null) {
            metrics.close();
        }
        if (lakeLookupExecutor != null) {
            shutdownExecutor("lake fallback lookup", lakeLookupExecutor);
        }
        if (timeoutExecutor != null) {
            shutdownExecutor("lake fallback timeout", timeoutExecutor);
        }
        Exception exception = null;
        if (flussLookupClient != null) {
            try {
                flussLookupClient.close();
            } catch (Exception e) {
                exception = e;
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private void shutdownExecutor(String executorName, ExecutorService executor) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(lakeFallbackTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warn(
                        "Timed out waiting {} for {} executor to terminate for table {}.",
                        lakeFallbackTimeout,
                        executorName,
                        tablePath);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn(
                    "Interrupted while waiting for {} executor to terminate for table {}.",
                    executorName,
                    tablePath,
                    e);
        }
    }

    private static class FlussLookupKey {
        private final Object[] primaryKeyValues;
        private final PartitionSpec partitionSpec;
        private final String partitionValue;

        private FlussLookupKey(
                Object[] primaryKeyValues, PartitionSpec partitionSpec, String partitionValue) {
            this.primaryKeyValues = primaryKeyValues;
            this.partitionSpec = partitionSpec;
            this.partitionValue = partitionValue;
        }
    }
}
