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
public final class SessionPropertyCompressionStrategyTest {

    private CompressionStrategy tablePropertyCompressionStrategy;

    @Before
    public void setup() {
        tablePropertyCompressionStrategy = new SessionPropertyCompressionStrategyImpl("avro.output.codec");
    }

    @Test
    public void testGetCompressionQueriesForUncompressedType() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "UNCOMPRESSED");
        assertEquals(2, compressionQueries.size());
        assertTrue(compressionQueries.contains("SET hive.exec.compress.output=false"));
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC"));
    }

    @Test
    public void testGetCompressionQueriesForSnappyAvro() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS AVRO");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "SNAPPY");
        assertEquals(3, compressionQueries.size());
        assertTrue(compressionQueries.contains("SET hive.exec.compress.output=true"));
        assertTrue(compressionQueries.contains("SET avro.output.codec=snappy"));
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS AVRO tblProperties(\"avro.output.codec\"=\"SNAPPY\")"));

    }

    @Test
    public void testGetCompressionQueriesForSnappyText() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS TEXTFILE");
        tablePropertyCompressionStrategy = new SessionPropertyCompressionStrategyImpl("text.compress");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "SNAPPY");
        assertEquals(4, compressionQueries.size());
        assertTrue(compressionQueries.contains("SET hive.exec.compress.output=true"));
        assertTrue(compressionQueries.contains("SET mapreduce.output.fileoutputformat.compress=true"));
        assertTrue(compressionQueries.contains("SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec"));
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS TEXTFILE tblProperties(\"text.compress\"=\"SNAPPY\")"));
    }

    @Test
    public void testGetCompressionQueriesForSnappySeq() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS SEQUENCEFILE");
        tablePropertyCompressionStrategy = new SessionPropertyCompressionStrategyImpl("seq.compress");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "SNAPPY");
        assertEquals(5, compressionQueries.size());
        assertTrue(compressionQueries.contains("SET hive.exec.compress.output=true"));
        assertTrue(compressionQueries.contains("SET mapred.output.compression.type=BLOCK"));
        assertTrue(compressionQueries.contains("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"));
        assertTrue(compressionQueries.contains("SET io.compression.codecs=org.apache.hadoop.io.compress.SnappyCodec"));
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS SEQUENCEFILE tblProperties(\"seq.compress\"=\"SNAPPY\")"));
    }

    @Test
    public void testGetCompressionQueriesForSnappyRC() {
        final StringBuilder createTableQuery = new StringBuilder("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS RCFILE");
        tablePropertyCompressionStrategy = new SessionPropertyCompressionStrategyImpl("rc.compress");
        final List<String> compressionQueries = tablePropertyCompressionStrategy.getCompressionQueries(createTableQuery, "SNAPPY");
        assertEquals(5, compressionQueries.size());
        assertTrue(compressionQueries.contains("SET hive.exec.compress.output=true"));
        assertTrue(compressionQueries.contains("SET mapred.output.compression.type=BLOCK"));
        assertTrue(compressionQueries.contains("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"));
        assertTrue(compressionQueries.contains("SET io.compression.codecs=org.apache.hadoop.io.compress.SnappyCodec"));
        assertTrue(compressionQueries.contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS RCFILE tblProperties(\"rc.compress\"=\"SNAPPY\")"));
    }
}
