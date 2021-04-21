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

import org.junit.Test;

/**
 * Tests for {@link HiveProperty}
 */
public final class HivePropertyTest {

    @Test
    public void testStaticQueries() {
        assertEquals("SET hive.exec.dynamic.partition.mode=nonstrict", HiveProperty.DYNAMIC_PARTITION_MODE.getQuery());
        assertEquals("SET hive.exec.dynamic.partition=true", HiveProperty.DYNAMIC_PARTITION.getQuery());

        assertEquals("SET hive.execution.engine=tez", HiveProperty.HIVE_TEZ_EXECUTION.getQuery());
        assertEquals("SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat", HiveProperty.HIVE_TEZ_INPUT_FORMAT.getQuery());
        assertEquals("SET hive.execution.engine=mr", HiveProperty.HIVE_MR_EXECUTION.getQuery());

        assertEquals("SET hive.merge.mapfiles=true", HiveProperty.MERGE_MAP_FILES.getQuery());
        assertEquals("SET hive.merge.mapredfiles=true", HiveProperty.MERGE_MAPRED_FILES.getQuery());
    }

    @Test
    public void testGetQueryWithNullValue() {
        assertEquals("SET property", new HiveProperty("property", null).getQuery());
    }

    @Test
    public void testGetQueryWithStringValue() {
        assertEquals("SET property=value", new HiveProperty("property", "value").getQuery());
    }

    @Test
    public void testGetQueryWithLongValue() {
        assertEquals("SET property=1", new HiveProperty("property", 1L).getQuery());
    }

    @Test
    public void testGetQueryWithBooleanValue() {
        assertEquals("SET property=true", new HiveProperty("property", true).getQuery());
    }
}
