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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.DiskWriteLockedException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Paimon implementation of {@link LakeTableLookuper} for primary-key tables.
 *
 * <p>The catalog, table, local query, and I/O manager are initialized lazily on the first lookup.
 * For each partition and bucket, the lookuper scans the latest Paimon snapshot once and registers
 * its data files with {@link LocalTableQuery}. Paimon then creates local lookup files lazily as
 * individual remote data files are queried.
 *
 * <p>A cached partition-bucket file set can become stale when Paimon compaction replaces its data
 * files and snapshot expiration physically deletes the old files. Because {@code FileIO}
 * implementations may represent a missing file with different {@link IOException} types, the first
 * lookup I/O failure refreshes that partition-bucket with the files from the latest snapshot and
 * retries once.
 *
 * <p>Lookup calls do not acquire a Fluss-level lock, allowing a Paimon version that supports
 * concurrent lookup to serve them concurrently. Lazy initialization is synchronized only on its
 * slow path. A version-aware lookup engine uses Paimon 1.3's query monitor or Paimon 2.0's internal
 * bucket locks as appropriate.
 *
 * <p>An explicit refresh request is applied during the next lookup initialization. It rescans every
 * registered partition-bucket and updates its file set in place. Paimon keeps lookup files for data
 * files that remain active and lazily downloads lookup files only for newly added data files.
 *
 * <p>Close is expected only after the owner has drained active lookups. It is synchronized with
 * lazy initialization and file-set updates, but deliberately does not add a lifecycle lock to every
 * lookup.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final String ioTmpDir;
    private final TableConfig tableConfig;
    private final long lookupCacheMaxDiskBytes;
    private final Runnable diskWriteGuard;

    private final ThreadLocal<Boolean> lookupFileDownloaded;
    private final Object initializationLock;
    // Remains non-zero until a refresh completes without observing another request.
    private final AtomicLong pendingRefreshRequests;

    private @Nullable Catalog catalog;
    private @Nullable IOManager ioManager;
    private @Nullable PaimonLookupRowConverter rowConverter;

    private volatile @Nullable PaimonLocalTableQuery localTableQuery;
    // Guarded by initializationLock.
    private volatile boolean closed;

    /** Creates a lookuper with the specified local lookup cache limit. */
    public PaimonLakeTableLookuper(
            Configuration paimonConfig,
            TablePath tablePath,
            String ioTmpDir,
            TableConfig tableConfig,
            long lookupCacheMaxDiskBytes,
            Runnable diskWriteGuard) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.ioTmpDir = checkNotNull(ioTmpDir, "ioTmpDir must not be null.");
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
        checkArgument(
                lookupCacheMaxDiskBytes > 0, "lookupCacheMaxDiskBytes must be greater than 0.");
        this.lookupCacheMaxDiskBytes = lookupCacheMaxDiskBytes;
        this.diskWriteGuard = checkNotNull(diskWriteGuard, "diskWriteGuard must not be null.");
        this.lookupFileDownloaded = new ThreadLocal<>();
        this.initializationLock = new Object();
        this.pendingRefreshRequests = new AtomicLong();
    }

    @Override
    public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        checkNotClosed();
        initialize(context.valueRowType());

        lookupFileDownloaded.set(false);
        long lookupStartNanos = System.nanoTime();
        try {
            return lookupInternal(key, context);
        } catch (Exception e) {
            DiskWriteLockedException diskWriteLockedException =
                    ExceptionUtils.findThrowable(e, DiskWriteLockedException.class).orElse(null);
            if (diskWriteLockedException != null) {
                throw diskWriteLockedException;
            }
            throw e;
        } finally {
            boolean fileDownloaded = lookupFileDownloaded.get();
            lookupFileDownloaded.remove();
            context.lookupMetricRecorder()
                    .recordLookup(System.nanoTime() - lookupStartNanos, fileDownloaded);
        }
    }

    @Override
    public void requestRefresh() {
        checkNotClosed();
        pendingRefreshRequests.incrementAndGet();
    }

    @Override
    public void close() {
        synchronized (initializationLock) {
            if (closed) {
                return;
            }
            closed = true;
            IOUtils.closeQuietly(localTableQuery, "Paimon lookup engine");
            IOUtils.closeQuietly(ioManager, "Paimon lookup IO manager");
            IOUtils.closeQuietly(catalog, "Paimon catalog");
            localTableQuery = null;
            rowConverter = null;
            ioManager = null;
            catalog = null;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Paimon lake table lookuper has been closed.");
        }
    }

    private void initialize(RowType valueRowType) throws Exception {
        if (localTableQuery == null || pendingRefreshRequests.get() != 0) {
            synchronized (initializationLock) {
                if (localTableQuery == null) {
                    initializeLookupState(valueRowType);
                }
                long observedRefreshRequests;
                while ((observedRefreshRequests = pendingRefreshRequests.get()) != 0) {
                    // A single scan handles all currently pending refresh requests.
                    checkNotNull(localTableQuery).refreshFilesFromLatestSnapshot();
                    // Clear the whole observed count only if it remained unchanged. If another
                    // refresh request arrived during the scan, the count was incremented, the CAS
                    // fails, and the next loop iteration performs another scan for that new
                    // refresh request.
                    pendingRefreshRequests.compareAndSet(observedRefreshRequests, 0);
                }
            }
        }
    }

    private void initializeLookupState(RowType valueRowType) throws Exception {
        Catalog newCatalog = null;
        IOManager newIOManager = null;
        LocalTableQuery newLocalTableQuery = null;
        boolean initialized = false;
        try {
            newCatalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
            FileStoreTable newFileStoreTable =
                    withLookupCacheOptions(
                            (FileStoreTable) newCatalog.getTable(toPaimon(tablePath)));
            PaimonLookupRowConverter newRowConverter =
                    new PaimonLookupRowConverter(
                            tableConfig, newFileStoreTable.schema(), valueRowType);

            newIOManager = createIOManager(ioTmpDir);
            newLocalTableQuery =
                    newFileStoreTable
                            .newLocalTableQuery()
                            .withValueProjection(newRowConverter.valueProjection())
                            .withIOManager(newIOManager);

            PaimonLocalTableQuery newLookupEngine =
                    new PaimonLocalTableQuery(newFileStoreTable, newLocalTableQuery);
            catalog = newCatalog;
            ioManager = newIOManager;
            rowConverter = newRowConverter;
            // Keep this volatile write last to publish all initialized fields together.
            localTableQuery = newLookupEngine;
            initialized = true;
        } finally {
            if (!initialized) {
                IOUtils.closeQuietly(newLocalTableQuery, "Paimon local table query");
                IOUtils.closeQuietly(newIOManager, "Paimon lookup IO manager");
                IOUtils.closeQuietly(newCatalog, "Paimon catalog");
            }
        }
    }

    private FileStoreTable withLookupCacheOptions(FileStoreTable table) {
        String key = CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE.key();
        String maxDiskSize = new MemorySize(lookupCacheMaxDiskBytes).toString();
        return table.copy(Collections.singletonMap(key, maxDiskSize));
    }

    private IOManager createIOManager(String ioTmpDir) {
        return new TrackingIOManager(IOManager.create(ioTmpDir));
    }

    private @Nullable byte[] lookupInternal(byte[] key, LookupContext context) {
        org.apache.paimon.data.InternalRow paimonRow;
        try {
            paimonRow =
                    localTableQuery.lookup(
                            rowConverter.getPartition(context),
                            context.bucketId(),
                            rowConverter.getKey(key, context));
        } catch (IOException e) {
            // Historical Paimon point lookup is part of the Fluss KV lookup path. Expose a
            // persistent I/O failure as a retriable KV error so the existing KV RPC retry
            // semantics can handle it consistently.
            throw new KvStorageException(
                    "Failed to lookup historical data from Paimon after refreshing files for "
                            + tablePath
                            + ".",
                    e);
        }
        if (paimonRow == null) {
            return null;
        }
        return rowConverter.encodeValue(paimonRow, context);
    }

    /** Tracks creation of Paimon lookup files while delegating all local I/O operations. */
    private final class TrackingIOManager implements IOManager {

        private final IOManager delegate;

        private TrackingIOManager(IOManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            return delegate.createChannel();
        }

        @Override
        public FileIOChannel.ID createChannel(String prefix) {
            try {
                diskWriteGuard.run();
            } catch (DiskWriteLockedException e) {
                // IOManager does not allow createChannel to declare IOException. Preserve the
                // I/O boundary here and unwrap the retriable Fluss exception in lookup().
                throw new UncheckedIOException(new IOException(e));
            }
            FileIOChannel.ID channel = delegate.createChannel(prefix);
            // Paimon creates lookup files synchronously in the lookup thread, so this marks only
            // the request that caused this channel to be created.
            if (lookupFileDownloaded.get() != null) {
                lookupFileDownloaded.set(true);
            }
            return channel;
        }

        @Override
        public String[] tempDirs() {
            return delegate.tempDirs();
        }

        @Override
        public String pickTempDir() {
            return delegate.pickTempDir();
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            return delegate.createChannelEnumerator();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileWriter(channelID);
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileReader(channelID);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
