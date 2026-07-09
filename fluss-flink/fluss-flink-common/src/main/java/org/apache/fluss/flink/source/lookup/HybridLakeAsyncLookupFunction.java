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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.PartitionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
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
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
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
import java.util.stream.IntStream;

import static org.apache.fluss.flink.utils.LakeSourceUtils.createLakeSource;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An async lookup function that falls back to lake data when cold Fluss lookup misses. */
public class HybridLakeAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLakeAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;
    private static final long LOOKUP_WINDOW_STATS_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final int LOOKUP_WINDOW_LATENCY_SAMPLE_SIZE = 8192;
    private static final long READABLE_LAKE_SNAPSHOT_CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(1);

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final int[] primaryKeyIndexes;
    private final int[] partitionKeyIndexes;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;
    private final Map<String, String> tableOptions;
    private final Duration hotWindow;
    private final ZoneId lookupTimeZone;
    @Nullable private final String autoPartitionKey;
    private final AutoPartitionTimeUnit autoPartitionTimeUnit;
    private final Duration lakeFallbackTimeout;
    private final int lakeFallbackExecutorThreads;
    private final int lakeFallbackMaxConcurrency;

    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient Connection connection;
    private transient Admin admin;
    private transient Table table;
    private transient Lookuper lookuper;
    private transient FlinkAsFlussRow lookupRow;
    private transient InternalRow.FieldGetter[] primaryKeyFieldGetters;
    private transient int autoPartitionKeyPositionInPrimaryKey;
    private transient org.apache.fluss.types.RowType flussFullRowType;
    private transient ThreadPoolExecutor lakeLookupExecutor;
    private transient ScheduledExecutorService timeoutExecutor;
    private transient ScheduledExecutorService windowStatsExecutor;
    private transient AtomicInteger lakeFallbackPendingCount;
    private transient PeriodicLookupStats periodicLookupStats;
    private transient Object readableLakeSnapshotCacheLock;
    @Nullable private transient CachedLakeSnapshot readableLakeSnapshotCache;
    private transient volatile boolean closed;
    private transient Counter lookupHotFlussHitsTotal;
    private transient Counter lookupHotFlussMissesTotal;
    private transient Counter lookupColdFlussHitsTotal;
    private transient Counter lookupColdFlussMissesTotal;
    private transient Counter lakeFallbackRequestsTotal;
    private transient Counter lakeFallbackHitsTotal;
    private transient Counter lakeFallbackMissesTotal;
    private transient Counter lakeFallbackFailuresTotal;
    private transient Counter lakeFallbackTimeoutsTotal;
    private transient Counter lakeFallbackRejectedTotal;
    private transient Histogram lakeFallbackLatencyMs;

    public HybridLakeAsyncLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            int[] primaryKeyIndexes,
            int[] partitionKeyIndexes,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            Map<String, String> tableOptions,
            Duration hotWindow,
            ZoneId lookupTimeZone,
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
        this.hotWindow = hotWindow;
        this.lookupTimeZone = lookupTimeZone;
        Configuration tableConfig = Configuration.fromMap(tableOptions);
        this.autoPartitionKey = tableConfig.getString(ConfigOptions.TABLE_AUTO_PARTITION_KEY);
        this.autoPartitionTimeUnit = tableConfig.get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT);
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
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
        table = connection.getTable(tablePath);
        lookupRow = new FlinkAsFlussRow();

        final RowType outputRowType;
        if (projection == null) {
            outputRowType = flinkRowType;
            projection = IntStream.range(0, flinkRowType.getFieldCount()).toArray();
        } else {
            outputRowType = FlinkUtils.projectRowType(flinkRowType, projection);
        }
        flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(outputRowType));

        Lookup lookup = table.newLookup();
        lookuper = lookup.createLookuper();

        org.apache.fluss.types.RowType primaryKeyRowType =
                flussFullRowType.project(primaryKeyIndexes);
        primaryKeyFieldGetters = new InternalRow.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyFieldGetters[i] =
                    InternalRow.createFieldGetter(primaryKeyRowType.getTypeAt(i), i);
        }
        autoPartitionKeyPositionInPrimaryKey = findAutoPartitionKeyPositionInPrimaryKey();

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
        periodicLookupStats = new PeriodicLookupStats();
        readableLakeSnapshotCacheLock = new Object();
        registerMetrics(context);
        windowStatsExecutor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-hybrid-lookup-window-stats"));
        startPeriodicStatsReporting();
        LOG.info("Finished opening hybrid lake async lookup function for table {}.", tablePath);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        LookupNormalizer.RemainingFilter remainingFilter =
                lookupNormalizer.createRemainingFilter(keyRow);
        FlussLookupKey lookupKey = createLookupKey(normalizedKeyRow);
        InternalRow flussKeyRow = lookupRow.replace(normalizedKeyRow);

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        if (closed) {
            future.complete(Collections.emptyList());
            return future;
        }
        long flussLookupStartMs = System.currentTimeMillis();
        lookuper.lookup(flussKeyRow)
                .whenComplete(
                        (result, throwable) -> {
                            if (closed) {
                                future.complete(Collections.emptyList());
                            } else if (throwable != null) {
                                periodicLookupStats.recordFlussFailure(
                                        elapsedMillis(flussLookupStartMs));
                                if (shouldFallbackToLakeOnFlussFailure(throwable, lookupKey)) {
                                    lookupColdFlussMissesTotal.inc();
                                    LOG.warn(
                                            "Fluss async lookup failed with invalid metadata for cold partition in table {}, partition {}, primary key indexes {}, primary key values {}. Falling back to lake lookup.",
                                            tablePath,
                                            lookupKey.partitionValue,
                                            Arrays.toString(primaryKeyIndexes),
                                            Arrays.toString(lookupKey.primaryKeyValues),
                                            throwable);
                                    lookupLakeAsync(lookupKey, remainingFilter, future);
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
                                periodicLookupStats.recordFlussCompletion(
                                        elapsedMillis(flussLookupStartMs), flussHit);
                                if (!isColdPartition(lookupKey.partitionValue)) {
                                    if (result.getRowList().isEmpty()) {
                                        lookupHotFlussMissesTotal.inc();
                                    } else {
                                        lookupHotFlussHitsTotal.inc();
                                    }
                                    handleLookupSuccess(future, result, remainingFilter);
                                } else if (!result.getRowList().isEmpty()) {
                                    lookupColdFlussHitsTotal.inc();
                                    handleLookupSuccess(future, result, remainingFilter);
                                } else {
                                    lookupColdFlussMissesTotal.inc();
                                    lookupLakeAsync(lookupKey, remainingFilter, future);
                                }
                            }
                        });
        return future;
    }

    private boolean shouldFallbackToLakeOnFlussFailure(
            Throwable throwable, FlussLookupKey lookupKey) {
        if (!ExceptionUtils.findThrowable(throwable, InvalidMetadataException.class).isPresent()) {
            return false;
        }
        try {
            return isColdPartition(lookupKey.partitionValue);
        } catch (RuntimeException e) {
            LOG.warn(
                    "Cannot determine whether Fluss lookup failure for table {}, partition {} can fall back to lake lookup.",
                    tablePath,
                    lookupKey.partitionValue,
                    e);
            return false;
        }
    }

    private void lookupLakeAsync(
            FlussLookupKey lookupKey,
            @Nullable LookupNormalizer.RemainingFilter remainingFilter,
            CompletableFuture<Collection<RowData>> future) {
        if (closed) {
            future.complete(Collections.emptyList());
            return;
        }
        lakeFallbackRequestsTotal.inc();
        periodicLookupStats.recordLakeRequest();
        lakeFallbackPendingCount.incrementAndGet();
        long startMs = System.currentTimeMillis();
        scheduleTimeout(future, lookupKey, startMs);
        try {
            lakeLookupExecutor.execute(
                    () -> {
                        periodicLookupStats.recordLakeStageLatency(
                                LakeLookupStage.QUEUE, elapsedMillis(startMs));
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
                                    lakeFallbackFailuresTotal,
                                    LakeFallbackOutcome.FAILURE,
                                    startMs);
                        }
                    });
        } catch (RuntimeException e) {
            completeLakeFallbackExceptionally(
                    future,
                    new RuntimeException("Lake fallback lookup executor is overloaded.", e),
                    lakeFallbackRejectedTotal,
                    LakeFallbackOutcome.REJECTED,
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
            if (rows.isEmpty()) {
                lakeFallbackMissesTotal.inc();
            } else {
                lakeFallbackHitsTotal.inc();
            }
            periodicLookupStats.recordLakeCompletion(latencyMs, !rows.isEmpty());
            recordLakeFallbackCompletion(latencyMs);
        }
    }

    private void completeLakeFallbackExceptionally(
            CompletableFuture<Collection<RowData>> future,
            Throwable throwable,
            Counter failureCounter,
            LakeFallbackOutcome outcome,
            long startMs) {
        if (future.completeExceptionally(throwable)) {
            long latencyMs = elapsedMillis(startMs);
            failureCounter.inc();
            periodicLookupStats.recordLakeFailure(latencyMs, outcome);
            recordLakeFallbackCompletion(latencyMs);
        }
    }

    private void completeLakeFallbackOnTimeout(
            CompletableFuture<Collection<RowData>> future,
            FlussLookupKey lookupKey,
            TimeoutException timeoutException,
            long startMs) {
        if (future.complete(Collections.emptyList())) {
            long latencyMs = elapsedMillis(startMs);
            lakeFallbackTimeoutsTotal.inc();
            periodicLookupStats.recordLakeFailure(latencyMs, LakeFallbackOutcome.TIMEOUT);
            recordLakeFallbackCompletion(latencyMs);
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

    private void recordLakeFallbackCompletion(long latencyMs) {
        lakeFallbackLatencyMs.update(latencyMs);
        lakeFallbackPendingCount.decrementAndGet();
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
            periodicLookupStats.recordLakeStageLatency(
                    LakeLookupStage.SNAPSHOT, elapsedMillis(stageStartMs));
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
            periodicLookupStats.recordLakeStageLatency(
                    LakeLookupStage.SOURCE_FILTER, elapsedMillis(stageStartMs));
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
            periodicLookupStats.recordLakeStageLatency(
                    LakeLookupStage.PLAN, elapsedMillis(stageStartMs));
        }

        if (closed) {
            return Collections.emptyList();
        }
        long splitLookupStartMs = System.currentTimeMillis();
        boolean splitLookupRecorded = false;
        LakeLookupFileStats fileStats = new LakeLookupFileStats(splits.size());
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
                        periodicLookupStats.recordLakeStageLatency(
                                LakeLookupStage.SPLIT_LOOKUP, elapsedMillis(splitLookupStartMs));
                        splitLookupRecorded = true;

                        stageStartMs = System.currentTimeMillis();
                        try {
                            RowData flinkRow =
                                    flussRowToFlinkRowConverter.toFlinkRowData(
                                            iterator.next().getRow());
                            if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                                return Collections.singletonList(flinkRow);
                            }
                        } finally {
                            periodicLookupStats.recordLakeStageLatency(
                                    LakeLookupStage.ROW_CONVERT_FILTER,
                                    elapsedMillis(stageStartMs));
                        }
                        splitLookupStartMs = System.currentTimeMillis();
                        splitLookupRecorded = false;
                    }
                }
            }
        } finally {
            periodicLookupStats.recordLakeFileStats(fileStats);
            if (!splitLookupRecorded) {
                periodicLookupStats.recordLakeStageLatency(
                        LakeLookupStage.SPLIT_LOOKUP, elapsedMillis(splitLookupStartMs));
            }
        }
        return Collections.emptyList();
    }

    private static class LakeLookupFileStats {
        private final int plannedSplits;
        private int matchedSplits;
        private long dataFiles;
        private long fileSizeBytes;
        private long rowCount;

        private LakeLookupFileStats(int plannedSplits) {
            this.plannedSplits = plannedSplits;
        }

        private void recordMatchedSplit(LakeSplit split) {
            matchedSplits++;
            collectDataFileStats(split);
        }

        private void collectDataFileStats(LakeSplit split) {
            try {
                Method dataSplitMethod = split.getClass().getMethod("dataSplit");
                Object dataSplit = dataSplitMethod.invoke(split);
                if (dataSplit == null) {
                    return;
                }
                Method dataFilesMethod = dataSplit.getClass().getMethod("dataFiles");
                Object dataFilesObject = dataFilesMethod.invoke(dataSplit);
                if (!(dataFilesObject instanceof Iterable)) {
                    return;
                }
                for (Object dataFile : (Iterable<?>) dataFilesObject) {
                    dataFiles++;
                    fileSizeBytes += invokeLong(dataFile, "fileSize");
                    rowCount += invokeLong(dataFile, "rowCount");
                }
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                // Lake split implementations are plugin-specific; file stats are best-effort.
            }
        }

        private static long invokeLong(Object target, String methodName)
                throws ReflectiveOperationException {
            Object value = target.getClass().getMethod(methodName).invoke(target);
            return value instanceof Number ? ((Number) value).longValue() : 0L;
        }
    }

    private boolean matchesLookupPartition(LakeSplit split, String partitionValue) {
        return split.partition().isEmpty() || split.partition().contains(partitionValue);
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

        List<RowData> projectedRows = new ArrayList<>();
        for (InternalRow row : lookupResult.getRowList()) {
            if (row != null) {
                RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(maybeProject(row));
                if (remainingFilter == null || remainingFilter.isMatch(flinkRow)) {
                    projectedRows.add(flinkRow);
                }
            }
        }
        resultFuture.complete(projectedRows);
    }

    private InternalRow maybeProject(InternalRow row) {
        if (projection == null) {
            return row;
        }
        return ProjectedRow.from(projection).replaceRow(row);
    }

    private FlussLookupKey createLookupKey(RowData normalizedKeyRow) {
        InternalRow row = new FlinkAsFlussRow(normalizedKeyRow);
        Object[] primaryKeyValues = new Object[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            primaryKeyValues[i] = primaryKeyFieldGetters[i].getFieldOrNull(row);
        }
        String partitionValue =
                String.valueOf(primaryKeyValues[autoPartitionKeyPositionInPrimaryKey]);
        if (primaryKeyValues[autoPartitionKeyPositionInPrimaryKey] instanceof BinaryString) {
            partitionValue = primaryKeyValues[autoPartitionKeyPositionInPrimaryKey].toString();
        }
        return new FlussLookupKey(primaryKeyValues, partitionValue);
    }

    private boolean isColdPartition(String partitionValue) {
        validateAutoPartitionTime(partitionValue);
        String hotWindowStartPartition =
                PartitionUtils.generateAutoPartitionTime(
                        ZonedDateTime.now(lookupTimeZone).minus(hotWindow),
                        0,
                        autoPartitionTimeUnit);
        return partitionValue.compareTo(hotWindowStartPartition) < 0;
    }

    private void validateAutoPartitionTime(String partitionValue) {
        try {
            DateTimeFormatter.ofPattern(getPartitionTimeFormat()).parse(partitionValue);
        } catch (DateTimeParseException e) {
            throw new TableException(
                    "Lake fallback lookup requires the partition value to match auto partition time unit '"
                            + autoPartitionTimeUnit
                            + "' with format '"
                            + getPartitionTimeFormat()
                            + "', but was: "
                            + partitionValue,
                    e);
        }
    }

    private String getPartitionTimeFormat() {
        switch (autoPartitionTimeUnit) {
            case YEAR:
                return "yyyy";
            case QUARTER:
                return "yyyyQ";
            case MONTH:
                return "yyyyMM";
            case DAY:
                return "yyyyMMdd";
            case HOUR:
                return "yyyyMMddHH";
            default:
                throw new TableException(
                        "Unsupported auto partition time unit for lake fallback lookup: "
                                + autoPartitionTimeUnit);
        }
    }

    private int findAutoPartitionKeyPositionInPrimaryKey() {
        int partitionKeyIndex = findAutoPartitionKeyIndex();
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            if (primaryKeyIndexes[i] == partitionKeyIndex) {
                return i;
            }
        }
        throw new TableException(
                "Lake fallback lookup requires auto partition key to be part of primary key.");
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

    private void registerMetrics(@Nullable FunctionContext context) {
        if (context == null) {
            lookupHotFlussHitsTotal = NoOpCounter.INSTANCE;
            lookupHotFlussMissesTotal = NoOpCounter.INSTANCE;
            lookupColdFlussHitsTotal = NoOpCounter.INSTANCE;
            lookupColdFlussMissesTotal = NoOpCounter.INSTANCE;
            lakeFallbackRequestsTotal = NoOpCounter.INSTANCE;
            lakeFallbackHitsTotal = NoOpCounter.INSTANCE;
            lakeFallbackMissesTotal = NoOpCounter.INSTANCE;
            lakeFallbackFailuresTotal = NoOpCounter.INSTANCE;
            lakeFallbackTimeoutsTotal = NoOpCounter.INSTANCE;
            lakeFallbackRejectedTotal = NoOpCounter.INSTANCE;
            lakeFallbackLatencyMs = NoOpHistogram.INSTANCE;
            return;
        }

        MetricGroup metricGroup = context.getMetricGroup();
        lookupHotFlussHitsTotal = metricGroup.counter(MetricNames.LOOKUP_HOT_FLUSS_HITS_TOTAL);
        lookupHotFlussMissesTotal = metricGroup.counter(MetricNames.LOOKUP_HOT_FLUSS_MISSES_TOTAL);
        lookupColdFlussHitsTotal = metricGroup.counter(MetricNames.LOOKUP_COLD_FLUSS_HITS_TOTAL);
        lookupColdFlussMissesTotal =
                metricGroup.counter(MetricNames.LOOKUP_COLD_FLUSS_MISSES_TOTAL);
        lakeFallbackRequestsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_REQUESTS_TOTAL);
        lakeFallbackHitsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_HITS_TOTAL);
        lakeFallbackMissesTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_MISSES_TOTAL);
        lakeFallbackFailuresTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_FAILURES_TOTAL);
        lakeFallbackTimeoutsTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_TIMEOUTS_TOTAL);
        lakeFallbackRejectedTotal = metricGroup.counter(MetricNames.LAKE_FALLBACK_REJECTED_TOTAL);
        lakeFallbackLatencyMs =
                metricGroup.histogram(
                        MetricNames.LAKE_FALLBACK_LATENCY_MS, new SlidingWindowHistogram(1024));
        metricGroup.gauge(MetricNames.LAKE_FALLBACK_PENDING_COUNT, lakeFallbackPendingCount::get);
        metricGroup.gauge(
                MetricNames.LAKE_FALLBACK_QUEUE_SIZE,
                () -> lakeLookupExecutor == null ? 0 : lakeLookupExecutor.getQueue().size());
    }

    private void startPeriodicStatsReporting() {
        windowStatsExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        reportLookupWindowStats(periodicLookupStats.rotate());
                    } catch (Throwable t) {
                        LOG.warn(
                                "Failed to report hybrid lookup window stats for table {}.",
                                tablePath,
                                t);
                    }
                },
                LOOKUP_WINDOW_STATS_INTERVAL_MS,
                LOOKUP_WINDOW_STATS_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    private void reportLookupWindowStats(WindowStatsSnapshot snapshot) {
        if (!snapshot.hasData()) {
            return;
        }
        LOG.info(
                "\nHybrid lookup 5-minute stats for table {} (intervalMs={}):\n{}",
                tablePath,
                snapshot.windowIntervalMs(),
                snapshot.toLogTable());
    }

    private static long elapsedMillis(long startMs) {
        return Math.max(0L, System.currentTimeMillis() - startMs);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing hybrid lake async lookup function for table {}.", tablePath);
        closed = true;
        if (periodicLookupStats != null) {
            reportLookupWindowStats(periodicLookupStats.rotate());
        }
        if (windowStatsExecutor != null) {
            shutdownExecutor("hybrid lookup window stats", windowStatsExecutor);
        }
        if (lakeLookupExecutor != null) {
            shutdownExecutor("lake fallback lookup", lakeLookupExecutor);
        }
        if (timeoutExecutor != null) {
            shutdownExecutor("lake fallback timeout", timeoutExecutor);
        }
        Exception exception = null;
        if (table != null) {
            try {
                table.close();
            } catch (Exception e) {
                exception = e;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
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

    private enum NoOpCounter implements Counter {
        INSTANCE;

        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return 0;
        }
    }

    private enum NoOpHistogram implements Histogram {
        INSTANCE;

        @Override
        public void update(long value) {}

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new SlidingWindowHistogramStatistics(new long[0]);
        }
    }

    private static class SlidingWindowHistogram implements Histogram {
        private final long[] values;
        private int position;
        private long count;

        private SlidingWindowHistogram(int size) {
            this.values = new long[size];
        }

        @Override
        public synchronized void update(long value) {
            values[position] = value;
            position = (position + 1) % values.length;
            count++;
        }

        @Override
        public synchronized long getCount() {
            return count;
        }

        @Override
        public synchronized HistogramStatistics getStatistics() {
            int size = (int) Math.min(count, values.length);
            long[] snapshot = new long[size];
            for (int i = 0; i < size; i++) {
                snapshot[i] = values[i];
            }
            return new SlidingWindowHistogramStatistics(snapshot);
        }
    }

    private static class SlidingWindowHistogramStatistics extends HistogramStatistics {
        private final long[] values;

        private SlidingWindowHistogramStatistics(long[] values) {
            this.values = values;
            Arrays.sort(this.values);
        }

        @Override
        public double getQuantile(double quantile) {
            if (values.length == 0) {
                return 0.0;
            }
            int index = (int) Math.ceil(quantile * values.length) - 1;
            index = Math.max(0, Math.min(index, values.length - 1));
            return values[index];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(values, values.length);
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public double getMean() {
            if (values.length == 0) {
                return 0.0;
            }
            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return (double) sum / values.length;
        }

        @Override
        public double getStdDev() {
            if (values.length <= 1) {
                return 0.0;
            }
            double mean = getMean();
            double sum = 0.0;
            for (long value : values) {
                double delta = value - mean;
                sum += delta * delta;
            }
            return Math.sqrt(sum / values.length);
        }

        @Override
        public long getMax() {
            return values.length == 0 ? 0 : values[values.length - 1];
        }

        @Override
        public long getMin() {
            return values.length == 0 ? 0 : values[0];
        }
    }

    private enum LakeFallbackOutcome {
        FAILURE,
        TIMEOUT,
        REJECTED
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

    private enum LakeLookupStage {
        QUEUE("queue"),
        SNAPSHOT("snapshot"),
        SOURCE_FILTER("sourceFilter"),
        PLAN("plan"),
        SPLIT_LOOKUP("splitLookup"),
        ROW_CONVERT_FILTER("rowConvertFilter");

        private final String displayName;

        LakeLookupStage(String displayName) {
            this.displayName = displayName;
        }
    }

    private static class PeriodicLookupStats {
        private final Object lock = new Object();

        private MutableWindowStats current = new MutableWindowStats(System.currentTimeMillis());

        private void recordFlussCompletion(long latencyMs, boolean hit) {
            synchronized (lock) {
                current.flussTotal++;
                if (hit) {
                    current.flussHits++;
                } else {
                    current.flussMisses++;
                }
                current.flussLatency.record(latencyMs);
            }
        }

        private void recordFlussFailure(long latencyMs) {
            synchronized (lock) {
                current.flussTotal++;
                current.flussFailures++;
                current.flussLatency.record(latencyMs);
            }
        }

        private void recordLakeRequest() {
            synchronized (lock) {
                current.lakeRequests++;
            }
        }

        private void recordLakeCompletion(long latencyMs, boolean hit) {
            synchronized (lock) {
                current.lakeCompletions++;
                if (hit) {
                    current.lakeHits++;
                } else {
                    current.lakeMisses++;
                }
                current.lakeLatency.record(latencyMs);
            }
        }

        private void recordLakeFailure(long latencyMs, LakeFallbackOutcome outcome) {
            synchronized (lock) {
                current.lakeCompletions++;
                current.lakeFailures++;
                if (outcome == LakeFallbackOutcome.TIMEOUT) {
                    current.lakeTimeouts++;
                } else if (outcome == LakeFallbackOutcome.REJECTED) {
                    current.lakeRejected++;
                }
                current.lakeLatency.record(latencyMs);
            }
        }

        private void recordLakeStageLatency(LakeLookupStage stage, long latencyMs) {
            synchronized (lock) {
                current.recordLakeStageLatency(stage, latencyMs);
            }
        }

        private void recordLakeFileStats(LakeLookupFileStats fileStats) {
            synchronized (lock) {
                current.recordLakeFileStats(fileStats);
            }
        }

        private WindowStatsSnapshot rotate() {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                WindowStatsSnapshot snapshot = current.toSnapshot(now);
                current = new MutableWindowStats(now);
                return snapshot;
            }
        }
    }

    private static class MutableWindowStats {
        private final long windowStartMs;
        private final SampledLatencyStats flussLatency =
                new SampledLatencyStats(LOOKUP_WINDOW_LATENCY_SAMPLE_SIZE);
        private final SampledLatencyStats lakeLatency =
                new SampledLatencyStats(LOOKUP_WINDOW_LATENCY_SAMPLE_SIZE);
        private final EnumMap<LakeLookupStage, SampledLatencyStats> lakeStageLatencies =
                new EnumMap<>(LakeLookupStage.class);
        private final LakeFileStatsAccumulator lakeFileStats = new LakeFileStatsAccumulator();

        private long flussTotal;
        private long flussHits;
        private long flussMisses;
        private long flussFailures;
        private long lakeRequests;
        private long lakeCompletions;
        private long lakeHits;
        private long lakeMisses;
        private long lakeFailures;
        private long lakeTimeouts;
        private long lakeRejected;

        private MutableWindowStats(long windowStartMs) {
            this.windowStartMs = windowStartMs;
            for (LakeLookupStage stage : LakeLookupStage.values()) {
                lakeStageLatencies.put(
                        stage, new SampledLatencyStats(LOOKUP_WINDOW_LATENCY_SAMPLE_SIZE));
            }
        }

        private void recordLakeStageLatency(LakeLookupStage stage, long latencyMs) {
            lakeStageLatencies.get(stage).record(latencyMs);
        }

        private void recordLakeFileStats(LakeLookupFileStats fileStats) {
            lakeFileStats.record(fileStats);
        }

        private WindowStatsSnapshot toSnapshot(long windowEndMs) {
            EnumMap<LakeLookupStage, LatencyStatsSnapshot> lakeStageLatencySnapshots =
                    new EnumMap<>(LakeLookupStage.class);
            for (Map.Entry<LakeLookupStage, SampledLatencyStats> entry :
                    lakeStageLatencies.entrySet()) {
                lakeStageLatencySnapshots.put(entry.getKey(), entry.getValue().toSnapshot());
            }
            return new WindowStatsSnapshot(
                    windowStartMs,
                    windowEndMs,
                    flussTotal,
                    flussHits,
                    flussMisses,
                    flussFailures,
                    flussLatency.toSnapshot(),
                    lakeRequests,
                    lakeCompletions,
                    lakeHits,
                    lakeMisses,
                    lakeFailures,
                    lakeTimeouts,
                    lakeRejected,
                    lakeLatency.toSnapshot(),
                    lakeStageLatencySnapshots,
                    lakeFileStats.toSnapshot());
        }
    }

    private static class LakeFileStatsAccumulator {
        private long samples;
        private long plannedSplitsSum;
        private long plannedSplitsMax;
        private long matchedSplitsSum;
        private long matchedSplitsMax;
        private long dataFilesSum;
        private long dataFilesMax;
        private long fileSizeBytesSum;
        private long fileSizeBytesMax;
        private long rowCountSum;
        private long rowCountMax;

        private void record(LakeLookupFileStats stats) {
            samples++;
            plannedSplitsSum += stats.plannedSplits;
            plannedSplitsMax = Math.max(plannedSplitsMax, stats.plannedSplits);
            matchedSplitsSum += stats.matchedSplits;
            matchedSplitsMax = Math.max(matchedSplitsMax, stats.matchedSplits);
            dataFilesSum += stats.dataFiles;
            dataFilesMax = Math.max(dataFilesMax, stats.dataFiles);
            fileSizeBytesSum += stats.fileSizeBytes;
            fileSizeBytesMax = Math.max(fileSizeBytesMax, stats.fileSizeBytes);
            rowCountSum += stats.rowCount;
            rowCountMax = Math.max(rowCountMax, stats.rowCount);
        }

        private LakeFileStatsSnapshot toSnapshot() {
            return new LakeFileStatsSnapshot(
                    samples,
                    plannedSplitsSum,
                    plannedSplitsMax,
                    matchedSplitsSum,
                    matchedSplitsMax,
                    dataFilesSum,
                    dataFilesMax,
                    fileSizeBytesSum,
                    fileSizeBytesMax,
                    rowCountSum,
                    rowCountMax);
        }
    }

    private static class SampledLatencyStats {
        private final long[] samples;
        private int position;
        private int sampleSize;
        private long count;
        private long sum;
        private long max;

        private SampledLatencyStats(int sampleCapacity) {
            this.samples = new long[sampleCapacity];
        }

        private void record(long value) {
            long latencyMs = Math.max(0L, value);
            count++;
            sum += latencyMs;
            max = Math.max(max, latencyMs);

            samples[position] = latencyMs;
            position = (position + 1) % samples.length;
            if (sampleSize < samples.length) {
                sampleSize++;
            }
        }

        private LatencyStatsSnapshot toSnapshot() {
            if (count == 0) {
                return LatencyStatsSnapshot.empty();
            }
            long[] snapshot = Arrays.copyOf(samples, sampleSize);
            Arrays.sort(snapshot);
            return new LatencyStatsSnapshot(
                    count,
                    (double) sum / count,
                    max,
                    quantile(snapshot, 0.50),
                    quantile(snapshot, 0.95),
                    quantile(snapshot, 0.99));
        }

        private static long quantile(long[] sortedValues, double quantile) {
            if (sortedValues.length == 0) {
                return 0L;
            }
            int index = (int) Math.ceil(quantile * sortedValues.length) - 1;
            index = Math.max(0, Math.min(index, sortedValues.length - 1));
            return sortedValues[index];
        }
    }

    private static class WindowStatsSnapshot {
        private final long windowStartMs;
        private final long windowEndMs;
        private final long flussTotal;
        private final long flussHits;
        private final long flussMisses;
        private final long flussFailures;
        private final LatencyStatsSnapshot flussLatency;
        private final long lakeRequests;
        private final long lakeCompletions;
        private final long lakeHits;
        private final long lakeMisses;
        private final long lakeFailures;
        private final long lakeTimeouts;
        private final long lakeRejected;
        private final LatencyStatsSnapshot lakeLatency;
        private final EnumMap<LakeLookupStage, LatencyStatsSnapshot> lakeStageLatencies;
        private final LakeFileStatsSnapshot lakeFileStats;

        private WindowStatsSnapshot(
                long windowStartMs,
                long windowEndMs,
                long flussTotal,
                long flussHits,
                long flussMisses,
                long flussFailures,
                LatencyStatsSnapshot flussLatency,
                long lakeRequests,
                long lakeCompletions,
                long lakeHits,
                long lakeMisses,
                long lakeFailures,
                long lakeTimeouts,
                long lakeRejected,
                LatencyStatsSnapshot lakeLatency,
                EnumMap<LakeLookupStage, LatencyStatsSnapshot> lakeStageLatencies,
                LakeFileStatsSnapshot lakeFileStats) {
            this.windowStartMs = windowStartMs;
            this.windowEndMs = windowEndMs;
            this.flussTotal = flussTotal;
            this.flussHits = flussHits;
            this.flussMisses = flussMisses;
            this.flussFailures = flussFailures;
            this.flussLatency = lakeFallbackSafe(flussLatency);
            this.lakeRequests = lakeRequests;
            this.lakeCompletions = lakeCompletions;
            this.lakeHits = lakeHits;
            this.lakeMisses = lakeMisses;
            this.lakeFailures = lakeFailures;
            this.lakeTimeouts = lakeTimeouts;
            this.lakeRejected = lakeRejected;
            this.lakeLatency = lakeFallbackSafe(lakeLatency);
            this.lakeStageLatencies = new EnumMap<>(LakeLookupStage.class);
            for (LakeLookupStage stage : LakeLookupStage.values()) {
                this.lakeStageLatencies.put(stage, lakeFallbackSafe(lakeStageLatencies.get(stage)));
            }
            this.lakeFileStats =
                    lakeFileStats == null ? LakeFileStatsSnapshot.empty() : lakeFileStats;
        }

        private static WindowStatsSnapshot empty() {
            EnumMap<LakeLookupStage, LatencyStatsSnapshot> emptyStageLatencies =
                    new EnumMap<>(LakeLookupStage.class);
            for (LakeLookupStage stage : LakeLookupStage.values()) {
                emptyStageLatencies.put(stage, LatencyStatsSnapshot.empty());
            }
            return new WindowStatsSnapshot(
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    LatencyStatsSnapshot.empty(),
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    LatencyStatsSnapshot.empty(),
                    emptyStageLatencies,
                    LakeFileStatsSnapshot.empty());
        }

        private long windowIntervalMs() {
            return Math.max(0L, windowEndMs - windowStartMs);
        }

        private boolean hasData() {
            return flussTotal > 0 || lakeRequests > 0 || lakeCompletions > 0;
        }

        private String toLogTable() {
            String[] headers = {
                "store",
                "requests",
                "total",
                "hits",
                "misses",
                "failures",
                "timeouts",
                "rejected",
                "avgMs",
                "maxMs",
                "p50Ms",
                "p95Ms",
                "p99Ms"
            };
            String[][] rows = {
                {
                    "fluss",
                    "-",
                    String.valueOf(flussTotal),
                    String.valueOf(flussHits),
                    String.valueOf(flussMisses),
                    String.valueOf(flussFailures),
                    "-",
                    "-",
                    formatLatency(flussLatency.avg),
                    String.valueOf(flussLatency.max),
                    String.valueOf(flussLatency.p50),
                    String.valueOf(flussLatency.p95),
                    String.valueOf(flussLatency.p99)
                },
                {
                    "lake",
                    String.valueOf(lakeRequests),
                    String.valueOf(lakeCompletions),
                    String.valueOf(lakeHits),
                    String.valueOf(lakeMisses),
                    String.valueOf(lakeFailures),
                    String.valueOf(lakeTimeouts),
                    String.valueOf(lakeRejected),
                    formatLatency(lakeLatency.avg),
                    String.valueOf(lakeLatency.max),
                    String.valueOf(lakeLatency.p50),
                    String.valueOf(lakeLatency.p95),
                    String.valueOf(lakeLatency.p99)
                }
            };
            int[] columnWidths = columnWidths(headers, rows);
            StringBuilder builder = new StringBuilder();
            appendRow(builder, headers, columnWidths);
            appendSeparator(builder, columnWidths);
            for (String[] row : rows) {
                appendRow(builder, row, columnWidths);
            }
            appendLakeStageLatencyTable(builder);
            appendLakeFileStatsTable(builder);
            return builder.toString();
        }

        private void appendLakeStageLatencyTable(StringBuilder builder) {
            boolean hasStageData = false;
            for (LatencyStatsSnapshot snapshot : lakeStageLatencies.values()) {
                if (snapshot.count > 0) {
                    hasStageData = true;
                    break;
                }
            }
            if (!hasStageData) {
                return;
            }

            builder.append('\n').append("lake stages:\n");
            String[] headers = {"stage", "samples", "avgMs", "maxMs", "p50Ms", "p95Ms", "p99Ms"};
            String[][] rows = new String[LakeLookupStage.values().length][headers.length];
            int rowIndex = 0;
            for (LakeLookupStage stage : LakeLookupStage.values()) {
                LatencyStatsSnapshot latency = lakeStageLatencies.get(stage);
                rows[rowIndex++] =
                        new String[] {
                            stage.displayName,
                            String.valueOf(latency.count),
                            formatLatency(latency.avg),
                            String.valueOf(latency.max),
                            String.valueOf(latency.p50),
                            String.valueOf(latency.p95),
                            String.valueOf(latency.p99)
                        };
            }
            int[] columnWidths = columnWidths(headers, rows);
            appendRow(builder, headers, columnWidths);
            appendSeparator(builder, columnWidths);
            for (String[] row : rows) {
                appendRow(builder, row, columnWidths);
            }
        }

        private void appendLakeFileStatsTable(StringBuilder builder) {
            if (lakeFileStats.samples == 0) {
                return;
            }

            builder.append('\n').append("lake file stats:\n");
            String[] headers = {"metric", "samples", "avg", "max"};
            String[][] rows = {
                lakeFileStats.toRow(
                        "plannedSplits",
                        lakeFileStats.plannedSplitsSum,
                        lakeFileStats.plannedSplitsMax),
                lakeFileStats.toRow(
                        "matchedSplits",
                        lakeFileStats.matchedSplitsSum,
                        lakeFileStats.matchedSplitsMax),
                lakeFileStats.toRow(
                        "dataFiles", lakeFileStats.dataFilesSum, lakeFileStats.dataFilesMax),
                lakeFileStats.toRow(
                        "fileSizeBytes",
                        lakeFileStats.fileSizeBytesSum,
                        lakeFileStats.fileSizeBytesMax),
                lakeFileStats.toRow(
                        "rowCount", lakeFileStats.rowCountSum, lakeFileStats.rowCountMax)
            };
            int[] columnWidths = columnWidths(headers, rows);
            appendRow(builder, headers, columnWidths);
            appendSeparator(builder, columnWidths);
            for (String[] row : rows) {
                appendRow(builder, row, columnWidths);
            }
        }

        private static int[] columnWidths(String[] headers, String[][] rows) {
            int[] widths = new int[headers.length];
            for (int i = 0; i < headers.length; i++) {
                widths[i] = headers[i].length();
            }
            for (String[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    widths[i] = Math.max(widths[i], row[i].length());
                }
            }
            return widths;
        }

        private static void appendRow(StringBuilder builder, String[] values, int[] widths) {
            builder.append("| ");
            for (int i = 0; i < values.length; i++) {
                String value =
                        i == 0 ? padRight(values[i], widths[i]) : padLeft(values[i], widths[i]);
                builder.append(value).append(" | ");
            }
            builder.setLength(builder.length() - 1);
            builder.append('\n');
        }

        private static void appendSeparator(StringBuilder builder, int[] widths) {
            builder.append("|");
            for (int width : widths) {
                builder.append(repeat('-', width + 2)).append("|");
            }
            builder.append('\n');
        }

        private static String padLeft(String value, int width) {
            if (value.length() >= width) {
                return value;
            }
            return repeat(' ', width - value.length()) + value;
        }

        private static String padRight(String value, int width) {
            if (value.length() >= width) {
                return value;
            }
            return value + repeat(' ', width - value.length());
        }

        private static String repeat(char character, int count) {
            char[] characters = new char[count];
            Arrays.fill(characters, character);
            return new String(characters);
        }

        private static String formatLatency(double latencyMs) {
            return String.format(Locale.ROOT, "%.2f", latencyMs);
        }

        private static LatencyStatsSnapshot lakeFallbackSafe(
                @Nullable LatencyStatsSnapshot snapshot) {
            return snapshot == null ? LatencyStatsSnapshot.empty() : snapshot;
        }
    }

    private static class LatencyStatsSnapshot {
        private final long count;
        private final double avg;
        private final long max;
        private final long p50;
        private final long p95;
        private final long p99;

        private LatencyStatsSnapshot(
                long count, double avg, long max, long p50, long p95, long p99) {
            this.count = count;
            this.avg = avg;
            this.max = max;
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
        }

        private static LatencyStatsSnapshot empty() {
            return new LatencyStatsSnapshot(0L, 0.0, 0L, 0L, 0L, 0L);
        }
    }

    private static class LakeFileStatsSnapshot {
        private final long samples;
        private final long plannedSplitsSum;
        private final long plannedSplitsMax;
        private final long matchedSplitsSum;
        private final long matchedSplitsMax;
        private final long dataFilesSum;
        private final long dataFilesMax;
        private final long fileSizeBytesSum;
        private final long fileSizeBytesMax;
        private final long rowCountSum;
        private final long rowCountMax;

        private LakeFileStatsSnapshot(
                long samples,
                long plannedSplitsSum,
                long plannedSplitsMax,
                long matchedSplitsSum,
                long matchedSplitsMax,
                long dataFilesSum,
                long dataFilesMax,
                long fileSizeBytesSum,
                long fileSizeBytesMax,
                long rowCountSum,
                long rowCountMax) {
            this.samples = samples;
            this.plannedSplitsSum = plannedSplitsSum;
            this.plannedSplitsMax = plannedSplitsMax;
            this.matchedSplitsSum = matchedSplitsSum;
            this.matchedSplitsMax = matchedSplitsMax;
            this.dataFilesSum = dataFilesSum;
            this.dataFilesMax = dataFilesMax;
            this.fileSizeBytesSum = fileSizeBytesSum;
            this.fileSizeBytesMax = fileSizeBytesMax;
            this.rowCountSum = rowCountSum;
            this.rowCountMax = rowCountMax;
        }

        private String[] toRow(String metric, long sum, long max) {
            return new String[] {
                metric, String.valueOf(samples), formatAverage(sum, samples), String.valueOf(max)
            };
        }

        private static String formatAverage(long sum, long count) {
            return count == 0 ? "0.00" : String.format(Locale.ROOT, "%.2f", (double) sum / count);
        }

        private static LakeFileStatsSnapshot empty() {
            return new LakeFileStatsSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }
    }

    private static class FlussLookupKey {
        private final Object[] primaryKeyValues;
        private final String partitionValue;

        private FlussLookupKey(Object[] primaryKeyValues, String partitionValue) {
            this.primaryKeyValues = primaryKeyValues;
            this.partitionValue = partitionValue;
        }
    }
}
