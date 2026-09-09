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

package org.apache.fluss.record;

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    @ParameterizedTest
    @CsvSource({"1, false", "1, true", "2, false", "2, true"})
    void testCompactedRecordsWithSchemaEvolution(int targetSchemaId, boolean readFromRemote)
            throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        Schema newSchema =
                Schema.newBuilder().fromSchema(oldSchema).column("added", DataTypes.INT()).build();
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(1, oldSchema);
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(newSchema, 2));
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test", "compacted_schema_evolution"),
                        1L,
                        targetSchemaId,
                        TableDescriptor.builder()
                                .schema(schemaGetter.getSchema(targetSchemaId))
                                .logFormat(LogFormat.COMPACTED)
                                .distributedBy(1)
                                .build(),
                        null,
                        0L,
                        0L);

        try (LogRecordReadContext context =
                LogRecordReadContext.createReadContext(
                        tableInfo, readFromRemote, null, schemaGetter)) {
            // Reuse one scanner context across batches written before and after ADD COLUMN.
            for (int batchSchemaId = 1; batchSchemaId <= 2; batchSchemaId++) {
                try (MemoryLogRecordsCompactedBuilder builder =
                        MemoryLogRecordsCompactedBuilder.builder(
                                10L,
                                batchSchemaId,
                                Integer.MAX_VALUE,
                                LOG_MAGIC_VALUE_V1,
                                new UnmanagedPagedOutputView(100))) {
                    for (int id = 0; id < 2; id++) {
                        Object[] fields =
                                batchSchemaId == 1
                                        ? new Object[] {id, "value" + id}
                                        : new Object[] {id, "value" + id, 100 + id};
                        builder.append(
                                ChangeType.INSERT,
                                compactedRow(
                                        schemaGetter.getSchema(batchSchemaId).getRowType(),
                                        fields));
                    }
                    LogRecordBatch batch =
                            MemoryLogRecords.pointToBytesView(builder.build())
                                    .batches()
                                    .iterator()
                                    .next();
                    try (CloseableIterator<LogRecord> records = batch.records(context)) {
                        for (int id = 0; id < 2; id++) {
                            assertThat(records.hasNext()).isTrue();
                            LogRecord record = records.next();
                            assertThat(record.logOffset()).isEqualTo(10L + id);
                            assertThat(record.timestamp()).isEqualTo(batch.commitTimestamp());
                            assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                            InternalRow row = record.getRow();
                            assertThat(row.getFieldCount()).isEqualTo(targetSchemaId == 1 ? 2 : 3);
                            assertThat(row.getInt(0)).isEqualTo(id);
                            assertThat(row.getString(1).toString()).isEqualTo("value" + id);
                            if (targetSchemaId == 2) {
                                assertThat(row.isNullAt(2)).isEqualTo(batchSchemaId == 1);
                                if (batchSchemaId == 2) {
                                    assertThat(row.getInt(2)).isEqualTo(100 + id);
                                }
                            }
                        }
                        assertThat(records.hasNext()).isFalse();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testRecordBatchSize(byte magic) throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(magic, TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testIndexedRowWriteAndReadBatch(byte magic) throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(ChangeType.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(allRowType).build(), schemaId));
        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                allRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }

        builder.close();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testNoRecordAppend(byte magic) throws Exception {
        // 1. no record append with baseOffset as 0.
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(baseRowType).build(), schemaId));
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }

        // 2. no record append with baseOffset as 100.
        builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        100L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));
        memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(100);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(101);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(100);

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }
}
