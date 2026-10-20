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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.TablePath;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Creates lake table lookupers and manages their shared resources within one TabletServer.
 *
 * <p>The caller must close all created lookupers after their requests have finished before closing
 * this manager. Closing a lookuper must not release resources shared with other lookupers.
 *
 * @since 1.1
 */
@PublicEvolving
public interface LakeTableLookuperManager extends AutoCloseable {

    /**
     * Creates a table-level point lookuper for the specified lake table.
     *
     * @param tablePath the logical path identifying the table in the lakehouse storage
     * @param context runtime context for creating the lookuper
     * @return a table-level point lookuper
     */
    LakeTableLookuper createLakeTableLookuper(TablePath tablePath, Context context);

    /**
     * Applies a new snapshot of the runtime resource settings to existing and future lookupers
     * without replacing them.
     *
     * <p>This method may be called concurrently with lookuper creation and lookups. Implementations
     * must apply the settings in a thread-safe manner. Lake-format and table-specific configuration
     * is outside the scope of this method.
     *
     * @param options the new runtime resource settings
     */
    void reconfigure(LookupCacheOptions options);

    /** Returns the cumulative number of lookup files evicted by the shared disk-space budget. */
    default long fileCacheCapacityEvictions() {
        return 0L;
    }

    /** Samples disk health and schedules cache reclamation when a disk is under pressure. */
    default void checkDiskUsage() {}

    /** Returns runtime-owned gauges keyed by their names within the historical metric group. */
    default Map<String, LongSupplier> cacheMetrics() {
        return Collections.emptyMap();
    }

    /**
     * Retains shared resources for a lookup batch, including gaps between its individual lookups.
     *
     * <p>The lease must be closed by the acquiring thread. Existing leases may finish after this
     * manager is closed; new acquisitions must then be rejected by resource-owning implementations.
     */
    default AutoCloseable retain() {
        return () -> {};
    }

    /**
     * Immutable snapshot of the format-independent resource settings for a lookup runtime.
     *
     * <p>These settings apply to resources shared by all table lookupers in one runtime.
     * Lake-format and table-specific configuration is supplied separately when creating a lookuper.
     */
    final class LookupCacheOptions {

        private final long localCacheMaxBytes;
        private final Duration expireAfterAccess;
        private final Map<String, Long> directoryMaxBytes;
        private final double diskWriteLimitRatio;
        private final double diskWriteRecoverRatio;

        /**
         * Creates runtime resource settings.
         *
         * @param localCacheMaxBytes positive disk-space budget in bytes for local caches shared by
         *     all lookupers in the runtime; implementations without local disk caches may ignore
         *     this budget
         * @param expireAfterAccess positive idle expiration for individual cached lookup files
         */
        public LookupCacheOptions(long localCacheMaxBytes, Duration expireAfterAccess) {
            this(
                    Collections.emptyMap(),
                    positiveBytes(localCacheMaxBytes),
                    expireAfterAccess,
                    1.0,
                    0.8);
        }

        /**
         * Creates per-volume settings. Each directory must belong to a distinct local volume.
         *
         * @param directoryMaxBytes cache roots and their positive resident-file budgets
         * @param expireAfterAccess positive idle expiration
         * @param diskWriteLimitRatio usage ratio at which cache writes stop; 1 disables this limit
         * @param diskWriteRecoverRatio usage ratio at which cache writes resume
         */
        public LookupCacheOptions(
                Map<String, Long> directoryMaxBytes,
                Duration expireAfterAccess,
                double diskWriteLimitRatio,
                double diskWriteRecoverRatio) {
            this(
                    Collections.unmodifiableMap(
                            new LinkedHashMap<>(
                                    checkNotNull(directoryMaxBytes, "directoryMaxBytes"))),
                    0L,
                    expireAfterAccess,
                    diskWriteLimitRatio,
                    diskWriteRecoverRatio);
        }

        private LookupCacheOptions(
                Map<String, Long> directoryMaxBytes,
                long fallbackMaxBytes,
                Duration expireAfterAccess,
                double diskWriteLimitRatio,
                double diskWriteRecoverRatio) {
            long totalBytes = fallbackMaxBytes;
            for (Map.Entry<String, Long> directory : directoryMaxBytes.entrySet()) {
                checkArgument(!directory.getKey().trim().isEmpty(), "Cache directory is empty.");
                totalBytes = Math.addExact(totalBytes, positiveBytes(directory.getValue()));
            }
            this.localCacheMaxBytes = totalBytes;
            this.directoryMaxBytes = directoryMaxBytes;
            this.expireAfterAccess =
                    checkNotNull(expireAfterAccess, "expireAfterAccess must not be null.");
            checkArgument(
                    !expireAfterAccess.isNegative() && !expireAfterAccess.isZero(),
                    "expireAfterAccess must be greater than 0.");
            checkArgument(
                    diskWriteRecoverRatio > 0.0
                            && diskWriteLimitRatio > diskWriteRecoverRatio
                            && diskWriteLimitRatio <= 1.0,
                    "Invalid disk write-limit and recovery ratios.");
            this.diskWriteLimitRatio = diskWriteLimitRatio;
            this.diskWriteRecoverRatio = diskWriteRecoverRatio;
        }

        /** Returns the runtime-wide disk-space budget for local caches, in bytes. */
        public long localCacheMaxBytes() {
            return localCacheMaxBytes;
        }

        /** Returns the idle expiration applied independently to each cached lookup file. */
        public Duration expireAfterAccess() {
            return expireAfterAccess;
        }

        /** Returns the per-volume cache roots and budgets, or an empty map for legacy callers. */
        public Map<String, Long> directoryMaxBytes() {
            return directoryMaxBytes;
        }

        /** Returns the high-water mark for cache writes. */
        public double diskWriteLimitRatio() {
            return diskWriteLimitRatio;
        }

        /** Returns the low-water mark for resuming cache writes. */
        public double diskWriteRecoverRatio() {
            return diskWriteRecoverRatio;
        }

        private static long positiveBytes(long bytes) {
            checkArgument(bytes > 0, "localCacheMaxBytes must be greater than 0.");
            return bytes;
        }
    }

    /** Runtime context for creating a lake table lookuper. */
    final class Context {
        private final Configuration lakeConfiguration;
        private final String cacheNamespace;
        private final TableConfig tableConfig;
        private final Runnable diskWriteGuard;

        /**
         * Creates a lookuper context.
         *
         * @param lakeConfiguration configuration of the lake storage for this lookuper
         * @param cacheNamespace namespace identifying cache entries owned by this lookuper
         * @param tableConfig configuration of the Fluss table
         * @param diskWriteGuard guard invoked before creating a local lookup cache file
         */
        public Context(
                Configuration lakeConfiguration,
                String cacheNamespace,
                TableConfig tableConfig,
                Runnable diskWriteGuard) {
            this.lakeConfiguration =
                    checkNotNull(lakeConfiguration, "lakeConfiguration must not be null.");
            this.cacheNamespace = checkNotNull(cacheNamespace, "cacheNamespace must not be null.");
            this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
            this.diskWriteGuard = checkNotNull(diskWriteGuard, "diskWriteGuard must not be null.");
        }

        /** Returns the lake storage configuration for this lookuper. */
        public Configuration lakeConfiguration() {
            return lakeConfiguration;
        }

        /** Returns the namespace identifying cache entries owned by this lookuper. */
        public String cacheNamespace() {
            return cacheNamespace;
        }

        /** Returns the configuration of the Fluss table. */
        public TableConfig tableConfig() {
            return tableConfig;
        }

        /** Returns the guard invoked before creating a local lookup cache file. */
        public Runnable diskWriteGuard() {
            return diskWriteGuard;
        }
    }
}
