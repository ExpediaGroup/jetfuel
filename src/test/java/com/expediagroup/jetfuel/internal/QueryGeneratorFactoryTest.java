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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.FileFormat;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;
import com.expediagroup.jetfuel.models.JetFuelRequest;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link QueryGeneratorFactory}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ QueryGenerator.class, Table.class })
public final class QueryGeneratorFactoryTest {

    private final HiveTableUtils client = mock(HiveTableUtils.class);
    private final Table table = mock(Table.class);
    private final JetFuelRequest request = new JetFuelRequest();

    private final JetFuelConfiguration.Builder builder = new JetFuelConfiguration.Builder()
            .withSourceDatabase("sourceDb")
            .withSourceTable("sourceTable")
            .withTargetDatabase("targetDb")
            .withTargetTable("targetTable")
            .withTargetFileFormat(FileFormat.ORC)
            .withTargetCompression("SNAPPY")
            .withMaxSplit("100000000")
            .withMinSplit("100000000")
            .withSizePerTask("800000000")
            .withSmallFileAvgSize("1000000000")
            .withPartitionGrouping("STATIC")
            .withHiveMetastoreUri("hiveMetastoreUri")
            .withHiveServer2Url("hiveUrl")
            .withMapReduceTaskTimeout("1110000")
            .withHiveServer2Username("username")
            .withHiveServer2Password("password");

    @Before
    public void setup() {
        when(client.getPartitions(table)).thenReturn("(partition1, partition2)");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullClient() {
        QueryGeneratorFactory.create(builder.build(), null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullJetFuelConfiguration() {
        QueryGeneratorFactory.create(null, client);
    }

    @Test
    public void testHiveCompactionSettings() {
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(builder.build(), client);
        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", true, false);
        assertTrue(request.getJetFuelQueries().contains("SET mapred.max.split.size=100000000"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.min.split.size=100000000"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.smallfiles.avgsize=1000000000"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.size.per.task=800000000"));
    }

    @Test
    public void testJetFuelRequestWithNullFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat((String) null)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(8, request.getJetFuelQueries().size());
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.dynamic.partition.mode=nonstrict"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.dynamic.partition=true"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.compress.output=false"));
        assertTrue(request.getJetFuelQueries().contains("DROP TABLE IF EXISTS targetDb.targetTable"));
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test(expected = NullPointerException.class)
    public void testGetInsertTableQueryNullTable() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(false, null, "cols", request);
    }

    @Test(expected = NullPointerException.class)
    public void testGetInsertTableQueryNullTableColumnsString() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(false, table, null, request);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInsertTableQueryEmptyTableColumnsString() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(false, table, "", request);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInsertTableQueryBlankTableColumnsString() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(false, table, "  ", request);
    }

    @Test
    public void testGetInsertTableQueryUnpartitioned() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(false, table, "cols", request);
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable SELECT * FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGetInsertTableQueryPartitioned() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGetInsertTableQueryPartitionedPartitionEmptyFilter() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withPartitionFilter("")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGetInsertTableQueryPartitionedPartitionFilter() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withPartitionFilter("partitionFilter")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE partitionFilter"));
    }

    @Test
    public void testGenerateJetFuelRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);
        assertEquals(7, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"SNAPPY\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelRequestWithDropTablePreFueling() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, false);
        assertEquals(5, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelCompactionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", true, true);
        assertEquals(13, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());

        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.mapfiles=true"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.mapredfiles=true"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.max.split.size=100000000"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.min.split.size=100000000"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.smallfiles.avgsize=1000000000"));
        assertTrue(request.getJetFuelQueries().contains("SET hive.merge.size.per.task=800000000"));

        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"SNAPPY\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetCompression("ZLIB")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(7, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"ZLIB\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelNoCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetCompression("")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);
        assertEquals(7, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"NONE\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelNullCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetCompression(null)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(7, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS ORC tblProperties(\"orc.compress\"=\"NONE\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelParquetCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .withTargetCompression("SNAPPY")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);
        assertEquals(7, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS PARQUET tblProperties(\"parquet.compression\"=\"SNAPPY\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelAvroCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("AVRO")
                .withTargetCompression("SNAPPY")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);
        assertEquals(9, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.compress.output=true"));
        assertTrue(request.getJetFuelQueries().contains("SET avro.output.codec=snappy"));
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS AVRO tblProperties(\"avro.output.codec\"=\"SNAPPY\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelTextFileCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("TEXT")
                .withTargetCompression("GZIP")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(10, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.compress.output=true"));
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.output.fileoutputformat.compress=true"));
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec"));
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS TEXTFILE tblProperties(\"text.compress\"=\"GZIP\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelSequenceCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("SEQ")
                .withTargetCompression("GZIP")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(11, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.compress.output=true"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.output.compression.type=BLOCK"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"));
        assertTrue(request.getJetFuelQueries().contains("SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec"));
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS SEQUENCEFILE tblProperties(\"seq.compress\"=\"GZIP\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGenerateJetFuelRCFileCompressionRequest() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("RC")
                .withTargetCompression("GZIP")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(11, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET hive.exec.compress.output=true"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.output.compression.type=BLOCK"));
        assertTrue(request.getJetFuelQueries().contains("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"));
        assertTrue(request.getJetFuelQueries().contains("SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec"));
        assertTrue(request.getJetFuelQueries().contains("CREATE TABLE targetDb.targetTable LIKE sourceDb.sourceTable STORED AS RCFILE tblProperties(\"rc.compress\"=\"GZIP\")"));
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    private void testStandardQueries(final List<String> queries) {
        assertTrue(queries.contains("SET hive.execution.engine=mr"));
        assertTrue(queries.contains("SET hive.exec.dynamic.partition.mode=nonstrict"));
        assertTrue(queries.contains("SET hive.exec.dynamic.partition=true"));

    }

    @Test
    public void testGenerateMapReduceMemoryQueries() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withMapReduceMemoryInMB(10L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(9, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.map.memory.mb=10"));
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.reduce.memory.mb=10"));
    }

    @Test
    public void testGenerateJavaOptsQueries() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withMapReduceJavaOptsInMB(10L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(9, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.map.java.opts=-Xmx10m"));
        assertTrue(request.getJetFuelQueries().contains("SET mapreduce.reduce.java.opts=-Xmx10m"));
    }

    @Test
    public void testGenerateParquetBlockSizeQuery() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withParquetBlockSize(10L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(8, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET parquet.block.size=10"));
    }

    @Test
    public void testGenerateParquetPageSizeQuery() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withParquetPageSize(10L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(8, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("SET parquet.page.size=10"));
    }

    @Test
    public void testGenerateConfigQueries() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withConfigQueries(ImmutableList.of("confQuery1", "confQuery2"))
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(true, table, "cols", false, true);

        assertEquals(9, request.getJetFuelQueries().size());
        testStandardQueries(request.getJetFuelQueries());
        assertTrue(request.getJetFuelQueries().contains("confQuery1"));
        assertTrue(request.getJetFuelQueries().contains("confQuery2"));
    }

    @Test
    public void testParquetWithNoPartitionFilter() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getInsertPartitionQueries().isEmpty());
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testGroupPartitionOverrideWithNoPartitionFilter() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withGroupPartitionOverride(true)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);
        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getInsertPartitionQueries().isEmpty());
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable"));
    }

    @Test
    public void testParquetSinglePartition() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .withPartitionFilter("partitionFilter")
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getInsertPartitionQueries().isEmpty());
        assertTrue(request.getJetFuelQueries().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE partitionFilter"));
    }

    @Test
    public void testParquetStaticGroupedPartitioned() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .withPartitionFilter("(trans_month = '2000-01') OR (trans_month = '2010-03')")
                .withInsertPartitionGroupSize(5L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertFalse(request.getInsertPartitionQueries().isEmpty());

        assertEquals(1, request.getInsertPartitionQueries().keySet().size());
        assertTrue(request.getInsertPartitionQueries().keySet().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2000-01')  OR  (trans_month = '2010-03')"));

        assertEquals(2, request.getInsertPartitionQueries().values().size());
        assertTrue(request.getInsertPartitionQueries().values().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2000-01') "));
        assertTrue(request.getInsertPartitionQueries().values().contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE  (trans_month = '2010-03')"));
    }

    @Test
    public void testParquetStaticGroupedPartitionedWithMultipleGroups() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .withPartitionFilter("(trans_month = '2000-01') OR (trans_month = '2010-03') OR (trans_month = '2011-03')")
                .withInsertPartitionGroupSize(2L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertFalse(request.getInsertPartitionQueries().isEmpty());

        assertEquals(2, request.getInsertPartitionQueries().keySet().size());
        final String groupQuery1 = "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2000-01')  OR  (trans_month = '2010-03') ";
        final String groupQuery2 = "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE  (trans_month = '2011-03')";

        assertTrue(request.getInsertPartitionQueries().keySet().contains(groupQuery1));
        assertEquals(2, request.getInsertPartitionQueries().get(groupQuery1).size());

        assertTrue(request.getInsertPartitionQueries().keySet().contains(groupQuery2));
        assertEquals(1, request.getInsertPartitionQueries().get(groupQuery2).size());

        assertEquals(3, request.getInsertPartitionQueries().values().size());
        assertTrue(request.getInsertPartitionQueries().get(groupQuery1).contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2000-01') "));
        assertTrue(request.getInsertPartitionQueries().get(groupQuery1).contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE  (trans_month = '2010-03') "));
        assertTrue(request.getInsertPartitionQueries().get(groupQuery2).contains("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE  (trans_month = '2011-03')"));
    }

    @Test
    public void testParquetDynamicGroupedPartitioned() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetFileFormat("PARQUET")
                .withPartitionFilter("(trans_month = '2000-01') OR (trans_month = '2010-03')")
                .withPartitionGrouping("DYNAMIC")
                .withInsertPartitionGroupSize(5L)
                .build();
        final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, client);

        queryGenerator.getInsertTableQuery(true, table, "cols", request);
        assertTrue(request.getInsertPartitionQueries().isEmpty());
        assertEquals(request.getInsertPartitionTemplate(), "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable");
        assertEquals(0, request.getInsertPartitionQueries().keySet().size());
        assertEquals(2, request.getPartitionFilterFragments().size());
        assertEquals(5, request.getPartitionGroupSize());
        assertEquals("(trans_month = '2000-01')", request.getPartitionFilterFragments().pop());
        assertEquals("(trans_month = '2010-03')", request.getPartitionFilterFragments().pop());
    }
}
