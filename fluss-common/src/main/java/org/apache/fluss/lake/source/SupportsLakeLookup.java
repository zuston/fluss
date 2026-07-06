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

import javax.annotation.Nullable;

import java.io.IOException;

/** Optional capability for lake sources that support primary-key point lookup. */
@PublicEvolving
public interface SupportsLakeLookup<Split extends LakeSplit> {

    /** Creates a reusable lookup object using the current projection of the lake source. */
    LakeLookup<Split> createLookup() throws IOException;

    /**
     * Creates a reusable lookup object using the current projection of the lake source and the
     * runtime IO temporary directory when the implementation needs one.
     */
    default LakeLookup<Split> createLookup(@Nullable String ioTmpDir) throws IOException {
        return createLookup();
    }
}
