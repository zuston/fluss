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

package org.apache.fluss.server;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletManagerBase}. */
final class TabletManagerBaseTest {

    @TempDir private File tempDir;

    @Test
    void testIgnoresHistoricalLookupCacheDirectoryWhenLoadingTablets() {
        File fakeTabletDir =
                new File(
                        new File(FlussPaths.historicalLookupRootDir(tempDir), "database"),
                        "kv-table-1");
        assertThat(fakeTabletDir.mkdirs()).isTrue();

        TestingTabletManager tabletManager = new TestingTabletManager(tempDir);

        assertThat(tabletManager.tabletsToLoad(tempDir)).isEmpty();
    }

    private static final class TestingTabletManager extends TabletManagerBase {
        private TestingTabletManager(File dataDir) {
            super(TabletType.KV, Collections.singletonList(dataDir), new Configuration(), 1);
        }

        private List<File> tabletsToLoad(File dataDir) {
            return listTabletsToLoad(dataDir);
        }
    }
}
