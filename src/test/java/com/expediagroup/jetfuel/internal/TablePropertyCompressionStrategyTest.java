/**
 * Copyright (C) 2018-2019 Expedia Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.jetfuel.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.JetFuelManager;
import com.expediagroup.jetfuel.JetFuelManagerFactory;
import com.expediagroup.jetfuel.models.CompressionStrategy;

/**
 * Tests for {@link JetFuelManagerFactory}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ JetFuelManagerFactory.class, JetFuelManager.class, JetFuelManagerImpl.class })
public final class TablePropertyCompressionStrategyTest {

    private CompressionStrategy tablePropertyCompressionStrategy;

    @Before
    public void setup() {
        tablePropertyCompressionStrategy = new TablePropertyCompressionStrategyImpl("orc.compress");
    }

    @Test
    public void testGetCompressionQueriesForUncompressedType() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "UNCOMPRESSED");
        assertEquals(1, compressionQueries.size());
        assertEquals(createTableQuery.toString(), compressionQueries.get(0));
    }

    @Test
    public void testGetCompressionQueriesForZLIB() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "ZLIB");
        assertEquals(1, compressionQueries.size());
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"ZLIB\")"));
    }
}
