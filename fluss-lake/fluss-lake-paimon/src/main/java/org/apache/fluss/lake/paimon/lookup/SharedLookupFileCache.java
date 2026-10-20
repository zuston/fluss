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
import org.apache.fluss.annotation.VisibleForTesting;

import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;

/** A weighted lookup-file cache shared by multiple Paimon table lookupers. */
@Internal
public final class SharedLookupFileCache implements AutoCloseable {

    private final Cache<Key, LookupFile> cache;
    private final AtomicLong capacityEvictions = new AtomicLong();
    private final BiConsumer<LookupFile, RemovalCause> fileRemoval;

    /** Creates a shared lookup-file cache. */
    public SharedLookupFileCache(Duration fileRetention, MemorySize maxDiskSize) {
        this(fileRetention, maxDiskSize, Ticker.systemTicker());
    }

    @VisibleForTesting
    SharedLookupFileCache(Duration fileRetention, MemorySize maxDiskSize, Ticker ticker) {
        this(
                fileRetention,
                maxDiskSize,
                ticker,
                (file, cause) -> {
                    try {
                        file.close(cause);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    SharedLookupFileCache(
            Duration fileRetention,
            MemorySize maxDiskSize,
            BiConsumer<LookupFile, RemovalCause> fileRemoval) {
        this(fileRetention, maxDiskSize, Ticker.systemTicker(), fileRemoval);
    }

    private SharedLookupFileCache(
            Duration fileRetention,
            MemorySize maxDiskSize,
            Ticker ticker,
            BiConsumer<LookupFile, RemovalCause> fileRemoval) {
        checkNotNull(fileRetention, "fileRetention must not be null.");
        checkNotNull(maxDiskSize, "maxDiskSize must not be null.");
        this.fileRemoval = checkNotNull(fileRemoval, "fileRemoval");
        this.cache =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(cacheWeight(maxDiskSize))
                        .weigher(
                                (Key key, LookupFile lookupFile) ->
                                        Math.max(1, fileKibiBytes(lookupFile.localFile())))
                        .ticker(checkNotNull(ticker, "ticker must not be null."))
                        .removalListener(this::removeLookupFile)
                        .executor(Runnable::run)
                        .build();
    }

    Cache<String, LookupFile> namespaced(String namespace) {
        return new NamespacedLookupFileCache(cache, namespace);
    }

    /** Updates the maximum cache weight. */
    public void updateMaxDiskSize(MemorySize maxDiskSize) {
        cache.policy().eviction().get().setMaximum(cacheWeight(maxDiskSize));
    }

    /** Updates idle expiration without replacing cached lookup files. */
    public void updateExpireAfterAccess(Duration expireAfterAccess) {
        cache.policy()
                .expireAfterAccess()
                .get()
                .setExpiresAfter(expireAfterAccess.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Returns the number of files evicted to enforce the shared disk-space budget. */
    public long capacityEvictions() {
        return capacityEvictions.get();
    }

    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    void evictDirectory(File directory, long bytesToFree) {
        if (bytesToFree <= 0) {
            return;
        }
        Map<Key, LookupFile> coldFiles =
                cache.policy()
                        .eviction()
                        .get()
                        .coldest((int) Math.min(Integer.MAX_VALUE, cache.estimatedSize()));
        long removedBytes = 0;
        for (Map.Entry<Key, LookupFile> entry : coldFiles.entrySet()) {
            LookupFile file = entry.getValue();
            if (file.localFile().toPath().startsWith(directory.toPath())) {
                long bytes = file.localFile().length();
                if (cache.asMap().remove(entry.getKey(), file)) {
                    capacityEvictions.incrementAndGet();
                    removedBytes += bytes;
                }
                if (removedBytes >= bytesToFree) {
                    break;
                }
            }
        }
    }

    void invalidateDirectory(File directory) {
        cache.asMap()
                .forEach(
                        (key, file) -> {
                            if (file.localFile().toPath().startsWith(directory.toPath())) {
                                cache.asMap().remove(key, file);
                            }
                        });
    }

    void invalidateFile(File localFile) {
        cache.asMap()
                .forEach(
                        (key, file) -> {
                            if (file.localFile().equals(localFile)) {
                                cache.asMap().remove(key, file);
                            }
                        });
    }

    void discard(LookupFile lookupFile) {
        removeLookupFile(null, lookupFile, RemovalCause.EXPLICIT);
    }

    private static long cacheWeight(MemorySize size) {
        return size.getBytes() == 0 ? 0 : Math.max(1L, size.getKibiBytes());
    }

    private void removeLookupFile(
            @Nullable Key key, @Nullable LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            if (cause == RemovalCause.SIZE) {
                capacityEvictions.incrementAndGet();
            }
            fileRemoval.accept(lookupFile, cause);
        }
    }

    static final class Key {
        final String namespace;
        final String fileName;

        Key(String namespace, String fileName) {
            this.namespace = checkNotNull(namespace, "namespace must not be null.");
            this.fileName = checkNotNull(fileName, "fileName must not be null.");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key key = (Key) o;
            return namespace.equals(key.namespace) && fileName.equals(key.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, fileName);
        }
    }
}
