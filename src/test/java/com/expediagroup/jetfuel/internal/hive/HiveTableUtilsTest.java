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
package com.expediagroup.jetfuel.internal.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link HiveTableUtils}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ HiveTableUtils.class, HiveMetaStoreClient.class })
public final class HiveTableUtilsTest {

    private final HiveMetaStoreClient hiveMetaStoreClient = mock(HiveMetaStoreClient.class);
    private final Table table = mock(Table.class);
    private final StorageDescriptor storageDescriptor = mock(StorageDescriptor.class);
    private final FieldSchema schema1 = new FieldSchema("col1", "int", "comment1");
    private final FieldSchema schema2 = new FieldSchema("col2", "smallint", "comment2");
    private HiveTableUtils hiveTableUtils;

    @Before
    public void setup() throws Exception {
        whenNew(HiveMetaStoreClient.class).withArguments(any()).thenReturn(hiveMetaStoreClient);
        when(hiveMetaStoreClient.getTable(anyString(), anyString())).thenReturn(table);
        when(table.getPartitionKeysSize()).thenReturn(100);
        when(table.getSd()).thenReturn(storageDescriptor);
        when(storageDescriptor.getCols()).thenReturn(ImmutableList.of(schema1, schema2));
        when(table.getPartitionKeys()).thenReturn(ImmutableList.of(schema1, schema2));
        hiveTableUtils = new HiveTableUtils(mock(HiveConf.class));
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullHiveConf() throws MetaException {
        new HiveTableUtils(null);
    }

    @Test(expected = NullPointerException.class)
    public void testGetTableNullDatabase() {
        hiveTableUtils.getTable(null, "table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableEmptyDatabase() {
        hiveTableUtils.getTable("", "table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableBlankDatabase() {
        hiveTableUtils.getTable("  ", "table");
    }

    @Test(expected = NullPointerException.class)
    public void testGetTableNullTableName() {
        hiveTableUtils.getTable("db", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableEmptyTableName() {
        hiveTableUtils.getTable("db", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableBlankTableName() {
        hiveTableUtils.getTable("db", "  ");
    }

    @Test
    public void testGetTable() {
        assertEquals(table, hiveTableUtils.getTable("db", "table"));
    }

    @Test(expected = JetFuelException.class)
    public void testGetTableError() throws TException {
        when(hiveMetaStoreClient.getTable(anyString(), anyString())).thenThrow(new IllegalArgumentException());
        hiveTableUtils.getTable("db", "table");
    }

    @Test(expected = NullPointerException.class)
    public void testIsPartitionedNullTable() {
        hiveTableUtils.isPartitioned(null);
    }

    @Test
    public void testIsPartitioned() {
        assertTrue(hiveTableUtils.isPartitioned(table));
    }

    @Test
    public void testIsNotPartitioned() {
        when(table.getPartitionKeysSize()).thenReturn(0);
        assertFalse(hiveTableUtils.isPartitioned(table));
    }

    @Test(expected = JetFuelException.class)
    public void testIsPartitionedError() {
        when(table.getPartitionKeysSize()).thenThrow(new IllegalArgumentException());
        assertTrue(hiveTableUtils.isPartitioned(table));
    }

    @Test(expected = NullPointerException.class)
    public void testGetTableColumnsAsStringNullTable() {
        hiveTableUtils.getTableColumnsAsString(null);
    }

    @Test
    public void testGetTableColumnsAsString() {
        assertEquals(String.format("%s, %s", schema1.getName(), schema2.getName()), hiveTableUtils.getTableColumnsAsString(table));
    }

    @Test(expected = JetFuelException.class)
    public void testGetTableColumnsAsStringError() {
        when(storageDescriptor.getCols()).thenThrow(new IllegalArgumentException());
        hiveTableUtils.getTableColumnsAsString(table);
    }

    @Test(expected = NullPointerException.class)
    public void testGetPartitionsNullTable() {
        hiveTableUtils.getPartitions(null);
    }

    @Test
    public void testGetPartitions() {
        assertEquals(String.format("(%s, %s)", schema1.getName(), schema2.getName()), hiveTableUtils.getPartitions(table));
    }

    @Test(expected = JetFuelException.class)
    public void testGetPartitionsError() {
        when(table.getPartitionKeys()).thenThrow(new IllegalArgumentException());
        hiveTableUtils.getPartitions(table);
    }

}
