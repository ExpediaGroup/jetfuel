/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.expediagroup.jetfuel.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.expediagroup.jetfuel.exception.JetFuelException;

import nl.jqno.equalsverifier.EqualsVerifier;

/**
 * Tests for {@link JetFuelConfiguration}
 */
public final class JetFuelConfigurationTest {

    private final JetFuelConfiguration.Builder builder = new JetFuelConfiguration.Builder()
            .withSourceDatabase("sourceDb")
            .withSourceTable("sourceTable")

            .withTargetDatabase("targetDb")
            .withTargetTable("targetTable")
            .withTargetCompaction(Boolean.FALSE)

            .withHiveMetastoreUri("hiveMetastoreUri")
            .withHiveServer2Url("hiveUrl")
            .withHiveServer2Username("username")
            .withHiveServer2Password("password")

            .withTargetFileFormat("ORC");

    @Test
    public void testEquals() {
        EqualsVerifier.forClass(JetFuelConfiguration.class).verify();
    }

    @Test
    public void testToString() {
        assertNotNull(builder.build().toString());
        assertTrue(builder.build().toString().startsWith("JetFuelConfiguration("));
    }

    @Test
    public void testValidateJetFuelConfigurationNullTargetCompaction() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetCompaction(null)
                .build();
    }

    @Test
    public void testValidateJetFuelConfigurationCompactionSettingsNull() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetCompaction(Boolean.TRUE)
                .build();

        assertEquals(jetFuelConfiguration.getMaxSplit(), "1024000000");
        assertEquals(jetFuelConfiguration.getMinSplit(), "256000000");
        assertEquals(jetFuelConfiguration.getSmallFileAvgSize(), "1024000000");
        assertEquals(jetFuelConfiguration.getSizePerTask(), "1000000000");
    }

    @Test
    public void testValidateJetFuelConfigurationCompactionSettingsBlank() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetCompaction(Boolean.TRUE)
                .withMaxSplit("")
                .withMinSplit("")
                .withSmallFileAvgSize("")
                .withSizePerTask("")
                .build();

        assertEquals(jetFuelConfiguration.getMaxSplit(), "1024000000");
        assertEquals(jetFuelConfiguration.getMinSplit(), "256000000");
        assertEquals(jetFuelConfiguration.getSmallFileAvgSize(), "1024000000");
        assertEquals(jetFuelConfiguration.getSizePerTask(), "1000000000");
    }

    @Test
    public void testValidateJetFuelConfigurationCompactionSettings() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetCompaction(Boolean.TRUE)
                .withMaxSplit("100")
                .withMinSplit("2")
                .withSmallFileAvgSize("1024")
                .withSizePerTask("4")
                .build();
        assertEquals(jetFuelConfiguration.getMaxSplit(), "100");
        assertEquals(jetFuelConfiguration.getMinSplit(), "2");
        assertEquals(jetFuelConfiguration.getSmallFileAvgSize(), "1024");
        assertEquals(jetFuelConfiguration.getSizePerTask(), "4");
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullSourceDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceDatabase(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptySourceDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceDatabase("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankSourceDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceDatabase("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullSourceTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceTable(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptySourceTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceTable("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankSourceTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceTable("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullTargetDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetDatabase(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptyTargetDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetDatabase("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankTargetDatabase() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetDatabase("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullTargetTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetTable(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptyTargetTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetTable("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankTargetTable() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetTable("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullHiveMetaStoreUri() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveMetastoreUri(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptyHiveMetaStoreUri() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveMetastoreUri("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankHiveMetaStoreUri() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveMetastoreUri("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullHiveServer2Url() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveServer2Url(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationEmptyHiveServer2Url() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveServer2Url("")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationBlankHiveServer2Url() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveServer2Url("  ")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateJetFuelConfigurationNullHiveServer2Username() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveServer2Username(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateJetFuelConfigurationNamingConstraint() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withSourceDatabase("dm")
                .withSourceTable("test")
                .withTargetDatabase("dm")
                .withTargetTable("test")
                .build();
    }

    @Test
    public void testValidateJetFuelConfigurationNullFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat((String) null)
                .build();

        assertEquals(FileFormat.NULL, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationNullHiveServer2Password() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withHiveServer2Password(null)
                .build();
    }

    @Test(expected = JetFuelException.class)
    public void testValidateJetFuelConfigurationInvalidFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("ABCD")
                .build();
    }

    @Test
    public void testValidateJetFuelConfigurationTextFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("TEXT")
                .build();
        assertEquals(FileFormat.TEXT, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationSeqFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("SEQ")
                .build();
        assertEquals(FileFormat.SEQ, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationRcFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("RC")
                .build();
        assertEquals(FileFormat.RC, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationAvroFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("AVRO")
                .build();
        assertEquals(FileFormat.AVRO, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationParquetFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("PARQUET")
                .build();
        assertEquals(FileFormat.PARQUET, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfiguration() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder.build();
        assertEquals(FileFormat.ORC, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testValidateJetFuelConfigurationEmptyFileFormat() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withTargetFileFormat("")
                .build();
        assertEquals(FileFormat.NULL, jetFuelConfiguration.getTargetFileFormat());
    }

    @Test
    public void testDefaultInsertPartitionGroupSize() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withInsertPartitionGroupSize(null)
                .build();
        assertEquals(5, jetFuelConfiguration.getInsertPartitionGroupSize(), 0);
    }

    @Test
    public void testDefaultInsertPartitionGroupSizeLessThan1() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withInsertPartitionGroupSize(0L)
                .build();
        assertEquals(5, jetFuelConfiguration.getInsertPartitionGroupSize(), 0);
    }

    @Test
    public void testInsertPartitionGroupSize() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder
                .withInsertPartitionGroupSize(10L)
                .build();
        assertEquals(10, jetFuelConfiguration.getInsertPartitionGroupSize(), 0);
    }

    @Test
    public void testIsGroupPartitionOverride() {
        final JetFuelConfiguration jetFuelConfiguration
                = builder.build();
        assertFalse(jetFuelConfiguration.isGroupPartitionOverride());
    }

    @Test
    public void testPreFueling() {
        final JetFuelConfiguration jetFuelConfigurationWithoutPreFueling = builder.build();
        assertNull(jetFuelConfigurationWithoutPreFueling.getPreFueling());

        final JetFuelConfiguration jetFuelConfigurationWithPreFueling = builder
                .withPreFueling(new PreFueling())
                .build();
        assertNotNull(jetFuelConfigurationWithPreFueling.getPreFueling());
    }

    @Test
    public void testWithStaticPartitionGrouping() {
        assertEquals(PartitionGrouping.NONE, builder
                .withPartitionGrouping(PartitionGrouping.STATIC)
                .build()
                .getPartitionGroupingStrategy());
        assertEquals(PartitionGrouping.STATIC, builder
                .withPartitionGrouping(PartitionGrouping.STATIC)
                .withEnablePartitionGrouping(true)
                .build()
                .getPartitionGroupingStrategy());
    }

    @Test
    public void testWithDynamicPartitionGrouping() {
        assertEquals(PartitionGrouping.NONE, builder
                .withPartitionGrouping(PartitionGrouping.DYNAMIC)
                .build()
                .getPartitionGroupingStrategy());
        assertEquals(PartitionGrouping.DYNAMIC, builder
                .withPartitionGrouping(PartitionGrouping.DYNAMIC)
                .withEnablePartitionGrouping(true)
                .build()
                .getPartitionGroupingStrategy());
    }

    @Test
    public void testWithStaticPartitionGroupingString() {
        assertEquals(PartitionGrouping.NONE, builder
                .withPartitionGrouping("static")
                .build()
                .getPartitionGroupingStrategy());
        assertEquals(PartitionGrouping.STATIC, builder
                .withPartitionGrouping("static")
                .withEnablePartitionGrouping(true)
                .build()
                .getPartitionGroupingStrategy());
    }

    @Test
    public void testWithDynamicPartitionGroupingString() {
        assertEquals(PartitionGrouping.NONE, builder
                .withPartitionGrouping("dynamic")
                .build()
                .getPartitionGroupingStrategy());
        assertEquals(PartitionGrouping.DYNAMIC, builder
                .withPartitionGrouping("dynamic")
                .withEnablePartitionGrouping(true)
                .build()
                .getPartitionGroupingStrategy());
    }

    @Test
    public void testTimeout() {
        final JetFuelConfiguration jetFuelConfigurationWithoutTimeout = builder
                .withMapReduceTaskTimeout(null)
                .build();
        assertNotNull(jetFuelConfigurationWithoutTimeout.getMapReduceTaskTimeout());
        assertEquals(Long.valueOf(1200000), jetFuelConfigurationWithoutTimeout.getMapReduceTaskTimeout());


        final JetFuelConfiguration jetFuelConfigurationWithTimeout = builder
                .withMapReduceTaskTimeout(Long.valueOf(110000))
                .build();
        assertNotNull(jetFuelConfigurationWithTimeout.getMapReduceTaskTimeout());
        assertEquals(Long.valueOf(110000), jetFuelConfigurationWithTimeout.getMapReduceTaskTimeout());
    }
}
