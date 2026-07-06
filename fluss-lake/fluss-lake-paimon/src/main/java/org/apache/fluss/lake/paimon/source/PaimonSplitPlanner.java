/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.Planner;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Split panner for paimon table. */
public class PaimonSplitPlanner implements Planner<PaimonSplit> {

    private final FileStoreTable fileStoreTable;
    private final @Nullable Predicate predicate;
    private final long snapshotId;

    public PaimonSplitPlanner(
            FileStoreTable fileStoreTable, @Nullable Predicate predicate, long snapshotId) {
        this.fileStoreTable = fileStoreTable;
        this.predicate = predicate;
        this.snapshotId = snapshotId;
    }

    @Override
    public List<PaimonSplit> plan() {
        try {
            List<PaimonSplit> splits = new ArrayList<>();
            FileStoreTable snapshotTable = getTable(snapshotId);
            InnerTableScan tableScan = snapshotTable.newScan();
            boolean isBucketUnAware = snapshotTable.bucketMode() == BucketMode.BUCKET_UNAWARE;

            if (predicate != null) {
                tableScan = tableScan.withFilter(predicate);
            }
            for (Split split : tableScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                splits.add(new PaimonSplit(dataSplit, isBucketUnAware));
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan splits for paimon.", e);
        }
    }

    private FileStoreTable getTable(long snapshotId) {
        return fileStoreTable.copy(
                Collections.singletonMap(
                        CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId)));
    }
}
