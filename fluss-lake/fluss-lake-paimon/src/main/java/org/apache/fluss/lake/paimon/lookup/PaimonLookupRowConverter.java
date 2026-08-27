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

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper.LookupContext;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.CompactedKeyDecoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.RowType;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartition;

/** Shared, thread-safe key, partition, and value conversion for Paimon lookupers. */
final class PaimonLookupRowConverter {

    private final TableConfig tableConfig;
    private final TableSchema schema;
    private final List<String> trimmedPrimaryKeys;
    private final int[] valueProjection;
    private final @Nullable CompactedKeyDecoder compactedKeyDecoder;

    PaimonLookupRowConverter(TableConfig tableConfig, TableSchema schema, RowType valueRowType) {
        if (schema.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Point lookup is only supported for primary-key Paimon tables.");
        }
        this.tableConfig = tableConfig;
        this.schema = schema;
        this.trimmedPrimaryKeys =
                Collections.unmodifiableList(new ArrayList<>(schema.trimmedPrimaryKeys()));
        this.valueProjection =
                IntStream.range(0, schema.fields().size())
                        .filter(i -> !SYSTEM_COLUMNS.containsKey(schema.fields().get(i).name()))
                        .toArray();

        // Only v2 tables with a non-default bucket key use compacted lookup keys. All others
        // already use Paimon's binary key encoding. Decode state is local to each invocation.
        this.compactedKeyDecoder =
                tableConfig.getKvFormatVersion().orElse(1) == KV_FORMAT_VERSION_2
                                && !schema.bucketKeys().equals(trimmedPrimaryKeys)
                        ? CompactedKeyDecoder.createKeyDecoder(valueRowType, trimmedPrimaryKeys)
                        : null;
    }

    int[] valueProjection() {
        return valueProjection;
    }

    org.apache.paimon.data.BinaryRow getPartition(LookupContext context) {
        // Generated helpers reuse mutable writers, so keep them confined to this lookup call.
        RowPartitionKeyExtractor partitionKeyExtractor = new RowPartitionKeyExtractor(schema);
        return toPaimonPartition(
                        context.partitionSpec(),
                        context.valueRowType(),
                        schema.logicalRowType(),
                        partitionKeyExtractor::partition)
                .copy();
    }

    org.apache.paimon.data.BinaryRow getKey(byte[] key, LookupContext context) {
        byte[] paimonKey = key;
        if (compactedKeyDecoder != null) {
            InternalRow decodedKey = compactedKeyDecoder.decodeKey(key);
            RowType keyRowType = context.valueRowType().project(trimmedPrimaryKeys);
            PaimonKeyEncoder paimonKeyEncoder =
                    new PaimonKeyEncoder(keyRowType, trimmedPrimaryKeys);
            paimonKey = paimonKeyEncoder.encodeKey(decodedKey);
        }

        org.apache.paimon.data.BinaryRow keyRow =
                new org.apache.paimon.data.BinaryRow(trimmedPrimaryKeys.size());
        keyRow.pointTo(MemorySegment.wrap(paimonKey), 0, paimonKey.length);
        return keyRow;
    }

    byte[] encodeValue(org.apache.paimon.data.InternalRow paimonRow, LookupContext context) {
        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);
        InternalRow.FieldGetter[] fieldGetters =
                InternalRow.createFieldGetters(context.valueRowType());
        try (RowEncoder rowEncoder =
                RowEncoder.create(tableConfig.getKvFormat(), context.valueRowType())) {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(flussRow));
            }
            BinaryRow row = rowEncoder.finishRow();
            return ValueEncoder.encodeValue(context.schemaId(), row);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode Paimon lookup row as Fluss value.", e);
        }
    }
}
