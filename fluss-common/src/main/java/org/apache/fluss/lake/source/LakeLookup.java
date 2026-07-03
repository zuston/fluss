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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** A lookup interface for lake sources that can point-query primary-key tables. */
@PublicEvolving
public interface LakeLookup<Split extends LakeSplit> extends Closeable {

    /** Refreshes the lookup view with the latest planned lake splits. */
    void refresh(List<Split> splits) throws IOException;

    /**
     * Looks up a primary key from the refreshed lake view.
     *
     * <p>The returned row follows the projection configured on the lake source that created this
     * lookup.
     */
    @Nullable
    InternalRow lookup(
            List<String> partitionValues,
            int bucket,
            Object[] primaryKeyValues,
            int[] primaryKeyIndexes)
            throws IOException;
}
