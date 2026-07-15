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
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/** Shared Fluss async lookup client used by regular and hybrid lookup functions. */
public class FlussAsyncLookupClient implements AutoCloseable {

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private int[] projection;
    private final boolean insertIfNotExists;

    private FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private Connection connection;
    private Table table;
    private Lookuper lookuper;
    private FlinkAsFlussRow lookupRow;

    public FlussAsyncLookupClient(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            boolean insertIfNotExists) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.insertIfNotExists = insertIfNotExists;
    }

    public void open() {
        connection = ConnectionFactory.createConnection(flussConfig);
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
        if (lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP) {
            int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
            RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
            lookup = lookup.lookupBy(lookupKeyRowType.getFieldNames());
        } else if (insertIfNotExists) {
            lookup = lookup.enableInsertIfNotExists();
        }
        lookuper = lookup.createLookuper();
    }

    public LookupRequest prepareLookup(RowData keyRow) {
        RowData normalizedKeyRow = lookupNormalizer.normalizeLookupKey(keyRow);
        RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
        InternalRow flussKeyRow = lookupRow.replace(normalizedKeyRow);
        return new LookupRequest(normalizedKeyRow, remainingFilter, flussKeyRow);
    }

    public CompletableFuture<LookupResult> lookup(InternalRow flussKeyRow) {
        return lookuper.lookup(flussKeyRow);
    }

    public Collection<RowData> toFlinkRows(
            LookupResult lookupResult, @Nullable RemainingFilter remainingFilter) {
        if (lookupResult.getRowList().isEmpty()) {
            return Collections.emptyList();
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
        return projectedRows;
    }

    private InternalRow maybeProject(InternalRow row) {
        return ProjectedRow.from(projection).replaceRow(row);
    }

    public FlussRowToFlinkRowConverter rowConverter() {
        return flussRowToFlinkRowConverter;
    }

    public int[] projection() {
        return projection;
    }

    public Table table() {
        return table;
    }

    public Connection connection() {
        return connection;
    }

    @Override
    public void close() throws Exception {
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

    /** Prepared lookup request. */
    public static class LookupRequest {
        private final RowData normalizedKeyRow;
        @Nullable private final RemainingFilter remainingFilter;
        private final InternalRow flussKeyRow;

        private LookupRequest(
                RowData normalizedKeyRow,
                @Nullable RemainingFilter remainingFilter,
                InternalRow flussKeyRow) {
            this.normalizedKeyRow = normalizedKeyRow;
            this.remainingFilter = remainingFilter;
            this.flussKeyRow = flussKeyRow;
        }

        public RowData normalizedKeyRow() {
            return normalizedKeyRow;
        }

        @Nullable
        public RemainingFilter remainingFilter() {
            return remainingFilter;
        }

        public InternalRow flussKeyRow() {
            return flussKeyRow;
        }
    }
}
