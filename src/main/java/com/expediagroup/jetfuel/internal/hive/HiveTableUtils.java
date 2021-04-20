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

import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.expediagroup.jetfuel.exception.JetFuelException;

import lombok.extern.slf4j.Slf4j;

/**
 * Hive Metastore client to retrive metadata information for tables
 */
@Slf4j
public class HiveTableUtils {

    private final HiveMetaStoreClient client;

    /**
     * Constructor
     *
     * @param hiveConf {@link HiveConf}
     * @throws MetaException thrown when unable to instantiate a HiveMetaStoreClient
     */
    public HiveTableUtils(final HiveConf hiveConf) throws MetaException {
        Validate.notNull(hiveConf, "HiveConf cannot be null");
        client = new HiveMetaStoreClient(hiveConf);
    }

    /**
     * Returns {@link Table} for table
     *
     * @param databaseName database name
     * @param tableName    table name
     * @return {@link Table}
     * @throws JetFuelException thrown when unable to retrieve table
     */
    public Table getTable(final String databaseName, final String tableName) throws JetFuelException {
        Validate.notBlank(databaseName, "DatabaseName cannot be null/empty/blank");
        Validate.notBlank(tableName, "TableName cannot be null/empty/blank");
        try {
            return client.getTable(databaseName, tableName);
        } catch (final Exception e) {
            final String errorMessage = String.format("Error retrieving table %s.%s: %s ", databaseName, tableName, e.getMessage());
            throw new JetFuelException(errorMessage, e);
        }
    }

    /**
     * Returns true when table is partitioned, false otherwise
     *
     * @param table {@link Table}
     * @return true when table is partitioned, false otherwise
     * @throws JetFuelException thrown when unable to retrieve partitioned status
     */
    public boolean isPartitioned(final Table table) throws JetFuelException {
        Validate.notNull(table, "Table cannot be null");
        try {
            return table.getPartitionKeysSize() != 0;
        } catch (final Exception e) {
            final String errorMessage = String.format("Error checking table %s.%s was partitioned: %s ", table.getDbName(), table.getTableName(), e.getMessage());
            throw new JetFuelException(errorMessage, e);
        }
    }

    /**
     * Retrieves column names for the table as a comma-separated string
     *
     * @param table {@link Table}
     * @return list of column names
     * @throws JetFuelException thrown when unable to retrieve columns
     */
    public String getTableColumnsAsString(final Table table) throws JetFuelException {
        Validate.notNull(table, "table cannot be null");
        try {
            return table.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.joining(", "));
        } catch (final Exception e) {
            final String errorMessage = String.format("Error retrieving table columns %s.%s: %s ", table.getDbName(), table.getTableName(), e.getMessage());
            throw new JetFuelException(errorMessage, e);
        }
    }

    /**
     * Retrieves partitions type names for table as a comma-separated string
     *
     * @param table {@link Table}
     * @return partition type names
     * @throws JetFuelException thrown when unable to retrieve partitions
     */
    public String getPartitions(final Table table) throws JetFuelException {
        Validate.notNull(table, "hiveTable cannot be null");
        try {
            return "(" + table.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.joining(", ")) + ")";
        } catch (final Exception e) {
            final String errorMessage = String.format("Error retrieving table partitions %s.%s: %s ", table.getDbName(), table.getTableName(), e.getMessage());
            throw new JetFuelException(errorMessage, e);
        }
    }
}
