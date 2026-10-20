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

import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.lookup.sort.SortLookupStoreReader;
import org.apache.paimon.lookup.sort.SortLookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.mergetree.lookup.RemoteFileDownloader;
import org.apache.paimon.utils.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;

/** Applies cache reservations and local-error attribution to both SST creation paths. */
final class LookupFileIO implements LookupStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LookupFileIO.class);

    private final DiskAwareIOManager ioManager;
    private final Comparator<MemorySlice> comparator;
    private final CacheManager cacheManager;
    private final int blockSize;
    private final @Nullable BlockCompressionFactory compressionFactory;

    LookupFileIO(
            DiskAwareIOManager ioManager,
            CoreOptions options,
            CacheManager cacheManager,
            Comparator<MemorySlice> comparator) {
        this.ioManager = ioManager;
        this.comparator = comparator;
        this.cacheManager = cacheManager;
        this.blockSize = options.cachePageSize();
        this.compressionFactory = BlockCompressionFactory.create(options.lookupCompressOptions());
    }

    @Override
    public LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException {
        PositionOutputStream output = openOutput(file);
        return new SortLookupStoreWriter(output, blockSize, bloomFilter, compressionFactory) {
            @Override
            public void close() throws IOException {
                // The native writer does not close its stream if flushing the footer fails.
                try (PositionOutputStream ignored = output) {
                    super.close();
                }
            }
        };
    }

    @Override
    public LookupStoreReader createReader(File file) throws IOException {
        DiskAwareIOManager.Allocation allocation = ioManager.allocation(file);
        allocation.ensureReadable();
        Path path = new Path(file.getAbsolutePath());
        SeekableInputStream input;
        try {
            input = LocalFileIO.INSTANCE.newInputStream(path);
        } catch (IOException e) {
            throw allocation.failure(e, true);
        }
        LookupStoreReader reader;
        try {
            reader =
                    new SortLookupStoreReader(comparator, path, file.length(), input, cacheManager);
        } catch (RuntimeException e) {
            IOUtils.closeQuietly(input);
            throw e;
        }
        allocation.reader = reader;
        return new LookupStoreReader() {
            @Override
            public byte[] lookup(byte[] key) throws IOException {
                allocation.ensureReadable();
                try {
                    return reader.lookup(key);
                } catch (IOException e) {
                    throw allocation.failure(e, true);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw allocation.failure(e, true);
                }
            }
        };
    }

    RemoteFileDownloader downloader(FileIO fileIO, DataFilePathFactory pathFactory) {
        return (DataFileMeta dataFile, String remoteName, File localFile) -> {
            Path remote = new Path(pathFactory.toPath(dataFile).getParent(), remoteName);
            try (SeekableInputStream input = fileIO.newInputStream(remote);
                    PositionOutputStream output = openOutput(localFile)) {
                byte[] buffer = new byte[64 << 10];
                int length;
                while ((length = input.read(buffer)) >= 0) {
                    output.write(buffer, 0, length);
                }
                return true;
            } catch (DiskAwareIOManager.CacheDiskIOException e) {
                // Returning false here would rebuild the SST on the same failing local volume.
                throw new UncheckedIOException(e);
            } catch (IOException e) {
                LOG.warn(
                        "Failed to download remote lookup file {}; rebuilding from lake data.",
                        remote,
                        e);
                return false;
            }
        };
    }

    private PositionOutputStream openOutput(File file) throws IOException {
        DiskAwareIOManager.Allocation allocation = ioManager.allocation(file);
        PositionOutputStream delegate;
        try {
            delegate = LocalFileIO.INSTANCE.newOutputStream(new Path(file.getAbsolutePath()), true);
            allocation.resetOutput();
        } catch (IOException e) {
            throw allocation.failure(e, false);
        }
        return new PositionOutputStream() {
            private boolean closed;

            @Override
            public long getPos() throws IOException {
                try {
                    return delegate.getPos();
                } catch (IOException e) {
                    throw allocation.failure(e, false);
                }
            }

            @Override
            public void write(int value) throws IOException {
                write(new byte[] {(byte) value});
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                write(bytes, 0, bytes.length);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                allocation.beforeWrite(length);
                try {
                    delegate.write(bytes, offset, length);
                    allocation.written(length);
                } catch (IOException e) {
                    throw allocation.failure(e, false);
                }
            }

            @Override
            public void flush() throws IOException {
                try {
                    delegate.flush();
                } catch (IOException e) {
                    throw allocation.failure(e, false);
                }
            }

            @Override
            public void close() throws IOException {
                if (!closed) {
                    closed = true;
                    try {
                        delegate.close();
                    } catch (IOException e) {
                        throw allocation.failure(e, false);
                    }
                }
            }
        };
    }
}
