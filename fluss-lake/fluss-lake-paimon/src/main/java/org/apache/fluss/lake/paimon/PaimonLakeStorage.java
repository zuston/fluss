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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.lakestorage.LakeTableLookuperManager;
import org.apache.fluss.lake.lakestorage.LakeTableLookuperManager.LookupCacheOptions;
import org.apache.fluss.lake.paimon.lookup.DiskAwareIOManager;
import org.apache.fluss.lake.paimon.lookup.PaimonLakeTableLookuper;
import org.apache.fluss.lake.paimon.lookup.PaimonScanBasedTableLookuper;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.paimon.source.PaimonSplit;
import org.apache.fluss.lake.paimon.tiering.PaimonCommittable;
import org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory;
import org.apache.fluss.lake.paimon.tiering.PaimonWriteResult;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.LakeLookupMode;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Paimon implementation of {@link LakeStorage}. */
public class PaimonLakeStorage implements LakeStorage {

    protected final Configuration paimonConfig;

    public PaimonLakeStorage(Configuration configuration) {
        this.paimonConfig = configuration;
    }

    @Override
    public LakeTieringFactory<PaimonWriteResult, PaimonCommittable> createLakeTieringFactory() {
        return new PaimonLakeTieringFactory(paimonConfig);
    }

    @Override
    public PaimonLakeCatalog createLakeCatalog() {
        return new PaimonLakeCatalog(paimonConfig);
    }

    @Override
    public LakeSource<PaimonSplit> createLakeSource(TablePath tablePath) {
        return new PaimonLakeSource(paimonConfig, tablePath);
    }

    @Override
    public LakeTableLookuperManager createLakeTableLookuperManager(
            String ioTmpDir, LookupCacheOptions options) {
        return new PaimonLakeTableLookuperManager(
                new LookupCacheOptions(
                        Collections.singletonMap(
                                new File(ioTmpDir).toPath().toAbsolutePath().normalize().toString(),
                                options.localCacheMaxBytes()),
                        options.expireAfterAccess(),
                        options.diskWriteLimitRatio(),
                        options.diskWriteRecoverRatio()),
                ForkJoinPool.commonPool());
    }

    @Override
    public LakeTableLookuperManager createLakeTableLookuperManager(
            LookupCacheOptions options, Executor maintenanceExecutor) {
        return new PaimonLakeTableLookuperManager(options, maintenanceExecutor);
    }

    /** Owns the shared I/O manager and file cache used by Paimon table lookupers. */
    private static final class PaimonLakeTableLookuperManager implements LakeTableLookuperManager {
        private final DiskAwareIOManager ioManager;

        private PaimonLakeTableLookuperManager(
                LookupCacheOptions options, Executor maintenanceExecutor) {
            checkNotNull(options, "options must not be null.");
            this.ioManager = new DiskAwareIOManager(options, maintenanceExecutor);
        }

        @Override
        public LakeTableLookuper createLakeTableLookuper(TablePath tablePath, Context context) {
            if (context.tableConfig().getHistoricalLookupMode() == LakeLookupMode.SCAN) {
                return new PaimonScanBasedTableLookuper(
                        new Configuration(context.lakeConfiguration()),
                        tablePath,
                        context.tableConfig());
            }
            return new PaimonLakeTableLookuper(
                    new Configuration(context.lakeConfiguration()),
                    tablePath,
                    ioManager,
                    context.cacheNamespace(),
                    context.tableConfig(),
                    context.diskWriteGuard());
        }

        @Override
        public void reconfigure(LookupCacheOptions options) {
            checkNotNull(options, "options must not be null.");
            ioManager.reconfigure(options);
        }

        @Override
        public void checkDiskUsage() {
            ioManager.checkDiskUsage();
        }

        @Override
        public Map<String, LongSupplier> cacheMetrics() {
            return ioManager.cacheMetrics();
        }

        @Override
        public AutoCloseable retain() {
            return ioManager.retain();
        }

        @Override
        public long fileCacheCapacityEvictions() {
            return ioManager.fileCache().capacityEvictions();
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(ioManager, "shared Paimon lookup IO manager");
        }
    }
}
