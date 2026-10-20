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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.DiskWriteLockedException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuperManager.LookupCacheOptions;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Routes lookup files to independently budgeted local volumes and owns their shared cache. */
@Internal
public final class DiskAwareIOManager implements IOManager {

    private static final Logger LOG = LoggerFactory.getLogger(DiskAwareIOManager.class);
    private static final long RESERVATION_BYTES = 8L << 20;

    private final List<Disk> disks = new ArrayList<>();
    private final ConcurrentMap<File, Allocation> files = new ConcurrentHashMap<>();
    private final ThreadLocal<LookupScope> currentLookup = new ThreadLocal<>();
    private final Executor maintenanceExecutor;
    private final SharedLookupFileCache fileCache;
    // Only selection and reservation bookkeeping hold this lock; never file I/O or cache removal.
    private final Object allocationLock = new Object();
    private volatile LookupCacheOptions cacheOptions;
    private int nextDisk;
    private int activeLookups;
    private volatile boolean closing;
    private boolean closed;

    /** Creates a shared lookup runtime with one native I/O manager per distinct volume. */
    public DiskAwareIOManager(LookupCacheOptions options, Executor maintenanceExecutor) {
        this.maintenanceExecutor = checkNotNull(maintenanceExecutor, "maintenanceExecutor");
        this.cacheOptions = checkNotNull(options, "options");
        Set<FileStore> volumes = new HashSet<>();
        for (Map.Entry<String, Long> directory : options.directoryMaxBytes().entrySet()) {
            Disk disk = new Disk(new File(directory.getKey()), directory.getValue());
            if (disk.volume != null && !volumes.add(disk.volume)) {
                IOUtils.closeQuietly(disk.delegate, "duplicate-volume lookup IO manager");
                LOG.warn(
                        "Ignoring lookup cache directory {} on an already configured volume.",
                        disk.directory);
            } else {
                disks.add(disk);
            }
        }
        this.fileCache =
                new SharedLookupFileCache(
                        options.expireAfterAccess(),
                        new MemorySize(cacheBudget()),
                        this::fileRemoved);
    }

    /** Returns the file cache shared by all table lookupers. */
    public SharedLookupFileCache fileCache() {
        return fileCache;
    }

    /** Updates budgets and watermarks without replacing cached table lookupers. */
    public void reconfigure(LookupCacheOptions options) {
        boolean legacySingleDisk =
                options.directoryMaxBytes().isEmpty()
                        && options.localCacheMaxBytes() > 0L
                        && disks.size() == 1;
        checkState(
                legacySingleDisk
                        || (options.directoryMaxBytes().size() == disks.size()
                                && disks.stream()
                                        .allMatch(
                                                disk ->
                                                        options.directoryMaxBytes()
                                                                .containsKey(
                                                                        disk.directory
                                                                                .getAbsolutePath()))),
                "Cache disk topology cannot change at runtime.");
        for (Disk disk : disks) {
            Long budget = options.directoryMaxBytes().get(disk.directory.getAbsolutePath());
            if (budget == null && options.directoryMaxBytes().isEmpty() && disks.size() == 1) {
                budget = options.localCacheMaxBytes();
            }
            checkState(budget != null, "Cache disk topology cannot change at runtime.");
            disk.maxBytes = budget;
        }
        cacheOptions = options;
        fileCache.updateMaxDiskSize(new MemorySize(cacheBudget()));
        fileCache.updateExpireAfterAccess(options.expireAfterAccess());
        checkDiskUsage();
    }

    /** Schedules one independent maintenance task per volume on the supplied I/O executor. */
    public void checkDiskUsage() {
        if (!closing) {
            disks.forEach(this::scheduleMaintenance);
        }
    }

    private void scheduleMaintenance(Disk disk) {
        if (!disk.maintenanceScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            maintenanceExecutor.execute(
                    () -> {
                        try {
                            disk.refresh();
                            if (disk.failed) {
                                fileCache.invalidateDirectory(disk.directory);
                            } else {
                                fileCache.evictDirectory(disk.directory, disk.bytesToFree());
                            }
                            Runnable removal;
                            while ((removal = disk.pendingRemovals.poll()) != null) {
                                removal.run();
                            }
                            if (!disk.failed) {
                                cleanOrphans(disk);
                                disk.refresh();
                            }
                        } catch (IOException e) {
                            disk.failed = true;
                            fileCache.invalidateDirectory(disk.directory);
                            LOG.warn("Could not check lookup cache volume {}.", disk.directory, e);
                        } finally {
                            disk.maintenanceScheduled.set(false);
                            if (!disk.pendingRemovals.isEmpty()) {
                                scheduleMaintenance(disk);
                            }
                        }
                    });
        } catch (RuntimeException e) {
            disk.maintenanceScheduled.set(false);
            LOG.warn("Could not schedule lookup cache maintenance for {}.", disk.directory, e);
        }
    }

    /** Returns gauges backed by the per-volume accounting, without performing I/O on reads. */
    public Map<String, LongSupplier> cacheMetrics() {
        Map<String, LongSupplier> metrics = new LinkedHashMap<>();
        for (int i = 0; i < disks.size(); i++) {
            Disk disk = disks.get(i);
            String prefix = "lookupCacheDisk" + i;
            metrics.put(prefix + "BudgetBytes", () -> disk.failed ? 0L : disk.maxBytes);
            metrics.put(prefix + "RetainedBytes", disk.retainedBytes::get);
            metrics.put(prefix + "ResidentBytes", () -> disk.residentBytes);
            metrics.put(prefix + "BuildingBytes", () -> disk.buildingBytes);
            metrics.put(prefix + "ReservedBytes", disk.reservedBytes::get);
            metrics.put(prefix + "OrphanBytes", () -> disk.orphanBytes);
            metrics.put(prefix + "State", () -> disk.failed ? 2L : disk.pressured ? 1L : 0L);
        }
        return metrics;
    }

    /** Retains the cache runtime for the current thread's complete lookup batch. */
    public AutoCloseable retain() {
        return openScope(false, () -> {});
    }

    LookupScope acquire(Runnable diskWriteGuard) {
        return openScope(true, diskWriteGuard);
    }

    private LookupScope openScope(boolean continuation, Runnable diskWriteGuard) {
        synchronized (this) {
            checkState(
                    !closing || (continuation && currentLookup.get() != null),
                    "Historical lookup cache is closed.");
            activeLookups++;
        }
        LookupScope scope = new LookupScope(currentLookup.get(), diskWriteGuard);
        currentLookup.set(scope);
        return scope;
    }

    Allocation allocate(String fileName, ConcurrentMap<String, LookupFile> ownedFiles)
            throws CacheDiskIOException {
        LookupScope scope = checkNotNull(currentLookup.get(), "Lookup scope is required.");
        checkState(scope.allocation == null, "A lookup file is already being created.");
        Allocation allocation;
        synchronized (allocationLock) {
            Disk selected = null;
            double lowest = Double.POSITIVE_INFINITY;
            for (int i = 0; i < disks.size(); i++) {
                Disk disk = disks.get((nextDisk + i) % disks.size());
                if (!disk.failed && !disk.pressured && !scope.failedDisks.contains(disk)) {
                    double load =
                            (double)
                                            (disk.residentBytes
                                                    + disk.buildingBytes
                                                    + disk.reservedBytes.get()
                                                    + disk.orphanBytes)
                                    / disk.maxBytes;
                    if (load < lowest) {
                        selected = disk;
                        lowest = load;
                    }
                }
            }
            allocation = selected == null ? null : new Allocation(selected, fileName, ownedFiles);
            if (selected != null) {
                nextDisk = (disks.indexOf(selected) + 1) % disks.size();
                allocation.reservedBytes = selected.reservationBytes;
                selected.reservedBytes.addAndGet(allocation.reservedBytes);
                scope.allocation = allocation;
            }
        }
        if (allocation == null) {
            throw unavailable();
        }
        try {
            allocation.disk.ensureSpace(0L);
            return allocation;
        } catch (IOException e) {
            allocation.abort();
            scope.allocation = null;
            scope.failedDisks.add(allocation.disk);
            if (e instanceof CacheDiskIOException) {
                throw (CacheDiskIOException) e;
            }
            throw allocation.failure(e, false);
        }
    }

    void finishAllocation(Allocation allocation) {
        currentLookup.get().allocation = null;
        synchronized (allocation.disk) {
            allocation.disk.reservedBytes.addAndGet(-allocation.reservedBytes);
            allocation.reservedBytes = 0L;
        }
    }

    Allocation allocation(File file) {
        return checkNotNull(files.get(file), "No lookup allocation for %s.", file);
    }

    boolean admit(LookupFile lookupFile) {
        Allocation allocation = allocation(lookupFile.localFile());
        Disk disk = allocation.disk;
        long bytes = allocation.bytes;
        if (bytes > disk.maxBytes || disk.failed || disk.pressured) {
            return false;
        }
        fileCache.evictDirectory(disk.directory, disk.retainedBytes.get() + bytes - disk.maxBytes);
        synchronized (disk) {
            if (disk.failed || disk.pressured || disk.retainedBytes.get() + bytes > disk.maxBytes) {
                return false;
            }
            disk.buildingBytes -= allocation.bytes;
            disk.residentBytes += bytes;
            disk.retainedBytes.addAndGet(bytes);
            allocation.visible.set(true);
            allocation.cached = true;
            return true;
        }
    }

    boolean retry(CacheDiskIOException failure) {
        LookupScope scope = currentLookup.get();
        Allocation allocation = failure.file == null ? null : files.get(failure.file);
        if (failure.disk != null
                && (allocation == null
                        || !allocation.cached
                        || failure.disk.failed
                        || failure.disk.pressured)) {
            scope.failedDisks.add(failure.disk);
        }
        if (failure.file != null) {
            fileCache.invalidateFile(failure.file);
        }
        return failure.disk != null && scope.localRetries++ < disks.size();
    }

    @Override
    public FileIOChannel.ID createChannel() {
        return createChannel("lookup");
    }

    @Override
    public FileIOChannel.ID createChannel(String prefix) {
        LookupScope scope = checkNotNull(currentLookup.get(), "Lookup scope is required.");
        Allocation allocation = checkNotNull(scope.allocation, "Allocation is required.");
        try {
            scope.diskWriteGuard.run();
        } catch (DiskWriteLockedException e) {
            throw new UncheckedIOException(new IOException(e));
        }
        try {
            allocation.disk.ensureSpace(0L);
            FileIOChannel.ID id = allocation.disk.delegate.createChannel(prefix);
            allocation.file = id.getPathFile();
            files.put(allocation.file, allocation);
            scope.fileCreated = true;
            return id;
        } catch (IOException e) {
            throw new UncheckedIOException(allocation.failure(e, false));
        }
    }

    @Override
    public String[] tempDirs() {
        return disks.stream().map(disk -> disk.directory.getAbsolutePath()).toArray(String[]::new);
    }

    @Override
    public String pickTempDir() {
        throw new UnsupportedOperationException("Lookup files require an allocation.");
    }

    @Override
    public FileIOChannel.Enumerator createChannelEnumerator() {
        throw new UnsupportedOperationException("Lookup files require an allocation.");
    }

    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) {
        throw new UnsupportedOperationException("Lookup SSTs use budgeted output streams.");
    }

    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) {
        throw new UnsupportedOperationException("Lookup SSTs use their own readers.");
    }

    @Override
    public void close() {
        boolean cleanup;
        synchronized (this) {
            closing = true;
            cleanup = activeLookups == 0 && !closed;
            closed |= cleanup;
        }
        if (cleanup) {
            closeResources();
        }
    }

    private void closeResources() {
        fileCache.close();
        for (Allocation allocation : files.values()) {
            if (allocation.lookupFile != null) {
                closeLookupFile(allocation.lookupFile);
            }
        }
        for (Disk disk : disks) {
            if (disk.sameVolume()) {
                IOUtils.closeQuietly(disk.delegate, "historical lookup disk IO manager");
            }
        }
    }

    private long cacheBudget() {
        return disks.stream().filter(disk -> !disk.failed).mapToLong(disk -> disk.maxBytes).sum();
    }

    private CacheDiskIOException unavailable() {
        boolean pressure = disks.stream().anyMatch(disk -> disk.pressured && !disk.failed);
        IOException cause =
                new IOException("No writable historical lookup cache volume is available.");
        if (pressure) {
            cause.initCause(new DiskWriteLockedException(cause.getMessage()));
        }
        checkDiskUsage();
        return new CacheDiskIOException(null, null, cause);
    }

    private void fileRemoved(LookupFile file, RemovalCause cause) {
        Allocation allocation = files.get(file.localFile());
        if (allocation == null) {
            return;
        }
        allocation.unlink();
        if (closing) {
            closeLookupFile(file, cause);
        } else {
            allocation.disk.pendingRemovals.add(() -> closeLookupFile(file, cause));
            scheduleMaintenance(allocation.disk);
        }
    }

    void closeLookupFile(LookupFile file) {
        closeLookupFile(file, RemovalCause.EXPLICIT);
    }

    private void closeLookupFile(LookupFile file, RemovalCause cause) {
        Allocation allocation = files.get(file.localFile());
        if (allocation == null) {
            return;
        }
        // Serialize physical close for this file, including an explicit owner close racing cleanup.
        synchronized (allocation) {
            allocation.unlink();
            IOException failure = null;
            try {
                if (allocation.disk.sameVolume()) {
                    file.close(cause);
                } else if (allocation.reader != null) {
                    // The original path may now refer to another mount. Only close its original FD.
                    allocation.reader.close();
                }
            } catch (IOException e) {
                failure = e;
            }
            allocation.ownedFiles.remove(allocation.fileName, file);
            allocation.release();
            if (failure != null) {
                LOG.warn("Failed to remove lookup cache file {}.", file.localFile(), failure);
            }
        }
    }

    private void cleanOrphans(Disk disk) {
        for (Allocation allocation : files.values()) {
            if (allocation.disk == disk && allocation.orphan && disk.sameVolume()) {
                try {
                    Files.deleteIfExists(allocation.file.toPath());
                    allocation.release();
                } catch (IOException e) {
                    LOG.debug("Lookup cache orphan {} remains on disk.", allocation.file, e);
                }
            }
        }
    }

    final class LookupScope implements AutoCloseable {
        private final @Nullable LookupScope previous;
        private final Set<Disk> failedDisks = new HashSet<>();
        private final Runnable diskWriteGuard;
        boolean fileCreated;
        private @Nullable Allocation allocation;
        private int localRetries;
        private boolean closed;

        private LookupScope(@Nullable LookupScope previous, Runnable diskWriteGuard) {
            this.previous = previous;
            this.diskWriteGuard = diskWriteGuard;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            checkState(
                    currentLookup.get() == this,
                    "Lookup leases must close in order on their acquiring thread.");
            closed = true;
            if (previous == null) {
                currentLookup.remove();
            } else {
                currentLookup.set(previous);
            }
            boolean cleanup;
            synchronized (DiskAwareIOManager.this) {
                activeLookups--;
                cleanup = closing && activeLookups == 0 && !closed;
                closed |= cleanup;
            }
            if (cleanup) {
                closeResources();
            }
        }
    }

    final class Allocation {
        private final Disk disk;
        private final String fileName;
        private final ConcurrentMap<String, LookupFile> ownedFiles;
        @Nullable private File file;
        @Nullable LookupStoreReader reader;
        @Nullable LookupFile lookupFile;
        private long bytes;
        private long reservedBytes;
        private volatile boolean cached;
        private final AtomicBoolean visible = new AtomicBoolean();
        private volatile boolean orphan;
        private boolean released;
        boolean outputOpened;

        private Allocation(
                Disk disk, String fileName, ConcurrentMap<String, LookupFile> ownedFiles) {
            this.disk = disk;
            this.fileName = fileName;
            this.ownedFiles = ownedFiles;
        }

        void resetOutput() {
            synchronized (disk) {
                disk.buildingBytes -= bytes;
                bytes = 0L;
                outputOpened = true;
            }
        }

        void beforeWrite(int length) throws IOException {
            try {
                synchronized (disk) {
                    if (disk.failed) {
                        throw new CacheDiskIOException(
                                disk, file, new IOException("Lookup cache volume is quarantined."));
                    }
                    if (reservedBytes < length) {
                        long extra = Math.max(disk.reservationBytes, length - reservedBytes);
                        disk.ensureSpace(extra);
                        reservedBytes += extra;
                        disk.reservedBytes.addAndGet(extra);
                    }
                }
            } catch (IOException e) {
                throw failure(e, false);
            }
        }

        void written(int length) {
            synchronized (disk) {
                bytes += length;
                disk.buildingBytes += length;
                reservedBytes -= length;
                disk.reservedBytes.addAndGet(-length);
            }
        }

        void ensureReadable() throws CacheDiskIOException {
            if (disk.failed) {
                throw new CacheDiskIOException(
                        disk, file, new IOException("Lookup cache volume is quarantined."));
            }
        }

        CacheDiskIOException failure(IOException cause, boolean reading) {
            if (cause instanceof CacheDiskIOException) {
                return (CacheDiskIOException) cause;
            }
            boolean missingOrCorrupt =
                    reading
                            && (cause instanceof FileNotFoundException
                                    || cause instanceof EOFException);
            if (!missingOrCorrupt) {
                LookupScope scope = currentLookup.get();
                if (scope != null) {
                    scope.failedDisks.add(disk);
                }
                disk.refresh();
                if (reading || !disk.pressured) {
                    disk.failed = true;
                    LOG.warn(
                            "Quarantining historical lookup cache volume {}.",
                            disk.directory,
                            cause);
                }
                checkDiskUsage();
            }
            return new CacheDiskIOException(disk, file, cause);
        }

        void abort() {
            if (file != null && disk.sameVolume()) {
                try {
                    Files.deleteIfExists(file.toPath());
                } catch (IOException e) {
                    LOG.warn("Failed to delete incomplete lookup file {}.", file, e);
                }
            }
            release();
        }

        private void unlink() {
            if (visible.compareAndSet(true, false)) {
                disk.retainedBytes.addAndGet(-bytes);
            }
        }

        private void release() {
            synchronized (disk) {
                if (released) {
                    return;
                }
                disk.reservedBytes.addAndGet(-reservedBytes);
                reservedBytes = 0L;
                if (cached) {
                    disk.residentBytes -= bytes;
                } else if (orphan) {
                    disk.orphanBytes -= bytes;
                } else {
                    disk.buildingBytes -= bytes;
                }
                cached = false;
                orphan = file != null && (!disk.sameVolume() || file.exists());
                if (orphan) {
                    if (disk.sameVolume()) {
                        bytes = Math.max(bytes, file.length());
                    }
                    disk.orphanBytes += bytes;
                } else {
                    released = true;
                    if (file != null) {
                        files.remove(file, this);
                    }
                }
            }
        }
    }

    static final class CacheDiskIOException extends IOException {
        private static final long serialVersionUID = 1L;
        private final @Nullable Disk disk;
        final @Nullable File file;

        private CacheDiskIOException(@Nullable Disk disk, @Nullable File file, IOException cause) {
            super(
                    "Historical lookup cache I/O failed"
                            + (file == null ? "." : " for " + file + "."),
                    cause);
            this.disk = disk;
            this.file = file;
        }
    }

    private final class Disk {
        private final File directory;
        private @Nullable FileStore volume;
        private @Nullable IOManager delegate;
        private long totalBytes;
        private volatile long maxBytes;
        private final AtomicLong retainedBytes = new AtomicLong();
        private volatile long residentBytes;
        private volatile long buildingBytes;
        private final AtomicLong reservedBytes = new AtomicLong();
        private long reservationBytes;
        private volatile long orphanBytes;
        private final AtomicBoolean maintenanceScheduled = new AtomicBoolean();
        private final Queue<Runnable> pendingRemovals = new ConcurrentLinkedQueue<>();
        private long requestedSpace;
        private volatile double usage;
        private volatile boolean pressured;
        private volatile boolean failed;

        private Disk(File directory, long maxBytes) {
            this.directory = directory.toPath().toAbsolutePath().normalize().toFile();
            this.maxBytes = maxBytes;
            try {
                Files.createDirectories(this.directory.toPath());
                volume = Files.getFileStore(this.directory.toPath());
                totalBytes = volume.getTotalSpace();
                checkState(totalBytes > 0L, "Cache volume reports no capacity.");
                reservationBytes = Math.max(1L, Math.min(RESERVATION_BYTES, totalBytes / 1024));
                delegate = IOManager.create(this.directory.getAbsolutePath());
                // Allocating an ID initializes native directories without creating a file.
                delegate.createChannel();
                refresh();
                LOG.info(
                        "Historical lookup cache uses {} with a {} byte budget.",
                        directory,
                        maxBytes);
            } catch (IOException | RuntimeException e) {
                failed = true;
                LOG.warn("Historical lookup cache directory {} is unavailable.", directory, e);
            }
        }

        private synchronized void refresh() {
            if (failed) {
                return;
            }
            try {
                if (!sameVolume()) {
                    throw new IOException("Lookup cache mount identity changed.");
                }
                usage = (double) (totalBytes - volume.getUsableSpace()) / totalBytes;
                LookupCacheOptions options = cacheOptions;
                boolean ratioPressure =
                        options.diskWriteLimitRatio() < 1.0
                                && (pressured
                                        ? usage > options.diskWriteRecoverRatio()
                                        : usage >= options.diskWriteLimitRatio());
                boolean spacePressure = writableSpace() < requestedSpace;
                if (!spacePressure) {
                    requestedSpace = 0L;
                }
                pressured = ratioPressure || spacePressure;
            } catch (IOException e) {
                failed = true;
                LOG.warn("Historical lookup cache volume {} failed its disk check.", directory, e);
            }
        }

        private synchronized void ensureSpace(long additionalBytes) throws IOException {
            refresh();
            if (failed) {
                throw new CacheDiskIOException(
                        this, null, new IOException("Lookup cache volume is unavailable."));
            }
            if (pressured || writableSpace() < additionalBytes) {
                requestedSpace = Math.max(requestedSpace, additionalBytes);
                pressured = true;
                throw new CacheDiskIOException(
                        this,
                        null,
                        new IOException(
                                new DiskWriteLockedException(
                                        "No space reserved for historical lookup cache on "
                                                + directory)));
            }
        }

        private long writableSpace() throws IOException {
            long keepFree =
                    Math.max(
                            reservationBytes,
                            (long)
                                    Math.ceil(
                                            (1.0 - cacheOptions.diskWriteLimitRatio())
                                                    * totalBytes));
            return volume.getUsableSpace() - keepFree - reservedBytes.get();
        }

        private synchronized long bytesToFree() throws IOException {
            long bytes = Math.max(0L, retainedBytes.get() - maxBytes);
            bytes = Math.max(bytes, requestedSpace - writableSpace());
            if (pressured && cacheOptions.diskWriteLimitRatio() < 1.0) {
                bytes =
                        Math.max(
                                bytes,
                                (long)
                                        Math.ceil(
                                                (usage - cacheOptions.diskWriteRecoverRatio())
                                                        * totalBytes));
            }
            return bytes;
        }

        private boolean sameVolume() {
            try {
                return volume != null && volume.equals(Files.getFileStore(directory.toPath()));
            } catch (IOException e) {
                return false;
            }
        }
    }
}
