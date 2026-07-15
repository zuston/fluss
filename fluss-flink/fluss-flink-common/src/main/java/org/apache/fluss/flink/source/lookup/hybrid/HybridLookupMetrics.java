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

import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Metrics and periodic window statistics for hybrid lake lookup. */
class HybridLookupMetrics implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HybridLookupMetrics.class);
    private static final long LOOKUP_WINDOW_STATS_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final int LOOKUP_WINDOW_LATENCY_SAMPLE_SIZE = 8192;

    private final TablePath tablePath;
    private final ThreadPoolExecutor lakeLookupExecutor;
    private final AtomicInteger lakeFallbackPendingCount;
    private final Duration shutdownTimeout;

    private Counter lookupHotFlussHitsTotal;
    private Counter lookupHotFlussMissesTotal;
    private Counter lookupColdFlussHitsTotal;
    private Counter lookupColdFlussMissesTotal;
    private Counter lakeFallbackRequestsTotal;
    private Counter lakeFallbackHitsTotal;
    private Counter lakeFallbackMissesTotal;
    private Counter lakeFallbackFailuresTotal;
    private Counter lakeFallbackTimeoutsTotal;
    private Counter lakeFallbackRejectedTotal;
    private Histogram lakeFallbackLatencyMs;

    private PeriodicLookupStats periodicLookupStats;
    private ScheduledExecutorService windowStatsExecutor;

    HybridLookupMetrics(
            TablePath tablePath,
            ThreadPoolExecutor lakeLookupExecutor,
            AtomicInteger lakeFallbackPendingCount,
            Duration shutdownTimeout) {
        this.tablePath = tablePath;
        this.lakeLookupExecutor = lakeLookupExecutor;
        this.lakeFallbackPendingCount = lakeFallbackPendingCount;
        this.shutdownTimeout = shutdownTimeout;
    }

    void open(@Nullable FunctionContext context) {
        periodicLookupStats = new PeriodicLookupStats();
        registerMetrics(context);
        windowStatsExecutor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("fluss-hybrid-lookup-window-stats"));
        startPeriodicStatsReporting();
    }

    void incHotFlussHits() {
        lookupHotFlussHitsTotal.inc();
    }

    void incHotFlussMisses() {
        lookupHotFlussMissesTotal.inc();
    }

    void incColdFlussHits() {
        lookupColdFlussHitsTotal.inc();
    }

    void incColdFlussMisses() {
        lookupColdFlussMissesTotal.inc();
    }

    void recordFlussCompletion(long latencyMs, boolean hit) {
        periodicLookupStats.recordFlussCompletion(latencyMs, hit);
    }

    void recordFlussFailure(long latencyMs) {
        periodicLookupStats.recordFlussFailure(latencyMs);
    }

    void recordLakeRequest() {
        lakeFallbackRequestsTotal.inc();
        periodicLookupStats.recordLakeRequest();
    }

    void recordLakeCompletion(long latencyMs, boolean hit) {
        if (hit) {
            lakeFallbackHitsTotal.inc();
        } else {
            lakeFallbackMissesTotal.inc();
        }
        periodicLookupStats.recordLakeCompletion(latencyMs, hit);
        lakeFallbackLatencyMs.update(latencyMs);
    }

    void recordLakeFailure(long latencyMs, LakeFallbackOutcome outcome) {
        switch (outcome) {
            case TIMEOUT:
                lakeFallbackTimeoutsTotal.inc();
                break;
            case REJECTED:
                lakeFallbackRejectedTotal.inc();
                break;
            case FAILURE:
                lakeFallbackFailuresTotal.inc();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported lake fallback outcome: " + outcome);
        }
        periodicLookupStats.recordLakeFailure(latencyMs, outcome);
        lakeFallbackLatencyMs.update(latencyMs);
    }

    void recordLakeStageLatency(LakeLookupStage stage, long latencyMs) {
        periodicLookupStats.recordLakeStageLatency(stage, latencyMs);
    }

    void recordLakeFileStats(LakeLookupFileStats fileStats) {
        periodicLookupStats.recordLakeFileStats(fileStats);
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

    @Override
    public void close() {
        if (periodicLookupStats != null) {
            reportLookupWindowStats(periodicLookupStats.rotate());
        }
        if (windowStatsExecutor != null) {
            windowStatsExecutor.shutdownNow();
            try {
                if (!windowStatsExecutor.awaitTermination(
                        shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    LOG.warn(
                            "Timed out waiting {} for hybrid lookup window stats executor to terminate for table {}.",
                            shutdownTimeout,
                            tablePath);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while waiting for hybrid lookup window stats executor to terminate for table {}.",
                        tablePath,
                        e);
            }
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

    enum LakeFallbackOutcome {
        FAILURE,
        TIMEOUT,
        REJECTED
    }

    enum LakeLookupStage {
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

    static class LakeLookupFileStats {
        private final int plannedSplits;
        private int matchedSplits;
        private long dataFiles;
        private long fileSizeBytes;
        private long rowCount;

        LakeLookupFileStats(int plannedSplits) {
            this.plannedSplits = plannedSplits;
        }

        void recordMatchedSplit(LakeSplit split) {
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
}
