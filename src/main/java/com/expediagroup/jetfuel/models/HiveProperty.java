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
package com.expediagroup.jetfuel.models;

public final class HiveProperty {
    public static final HiveProperty DYNAMIC_PARTITION_MODE = new HiveProperty("hive.exec.dynamic.partition.mode", "nonstrict");
    public static final HiveProperty DYNAMIC_PARTITION = new HiveProperty("hive.exec.dynamic.partition", "true");

    public static final HiveProperty HIVE_TEZ_EXECUTION = new HiveProperty("hive.execution.engine", "tez");
    public static final HiveProperty HIVE_TEZ_INPUT_FORMAT = new HiveProperty("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
    public static final HiveProperty HIVE_MR_EXECUTION = new HiveProperty("hive.execution.engine", "mr");

    public static final HiveProperty MERGE_MAP_FILES = new HiveProperty("hive.merge.mapfiles", "true");
    public static final HiveProperty MERGE_MAPRED_FILES = new HiveProperty("hive.merge.mapredfiles", "true");

    private final String name;
    private final Object value;

    /**
     * Constructor
     * @param name the property name
     * @param value the property value
     */
    public HiveProperty(final String name, final Object value) {
        this.name = name;
        this.value = value;
    }

    /**
     * @return Creates Hive Property with SET PropertyName=PropertyValue
     */
    public String getQuery() {
        if (value == null) {
            return String.format("SET %s", name);
        }
        return String.format("SET %s=%s", name, value.toString());
    }
}
