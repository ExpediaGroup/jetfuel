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


import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.metastore.api.Table;

import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.FileFormatCompressor;
import com.expediagroup.jetfuel.models.HiveProperty;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;
import com.expediagroup.jetfuel.models.JetFuelRequest;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/**
 * Manager class to generate queries for Jetfuel
 */
@Slf4j
public class QueryGenerator {

    private final HiveTableUtils hiveTableUtils;
    private final JetFuelConfiguration jetFuelConfiguration;
    private final FileFormatCompressor fileFormatCompressor;

    /**
     * Constructor
     *
     * @param hiveTableUtils {@link HiveTableUtils}
     * @param jetFuelConfiguration       {@link JetFuelConfiguration}
     * @param fileFormatCompressor {@link FileFormatCompressor}
     */
    QueryGenerator(final HiveTableUtils hiveTableUtils, final JetFuelConfiguration jetFuelConfiguration, final FileFormatCompressor fileFormatCompressor) {
        Validate.notNull(hiveTableUtils, "hiveTableUtils cannot be null");
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        Validate.notNull(fileFormatCompressor, "fileFormatCompressor cannot be null");

        this.hiveTableUtils = hiveTableUtils;
        this.jetFuelConfiguration = jetFuelConfiguration;
        this.fileFormatCompressor = fileFormatCompressor;
    }

    /**
     * Generate {@link JetFuelRequest} to perform JetFuel Operations
     *
     * @param isPartitioned        true when table is partitioned, false otherwise
     * @param table                {@link Table}
     * @param tableColumnsAsString the table columns as string
     * @param isCompacted          true when the table should be compacted, false if not specified
     * @param dropTablePreFueling  true when delete target table and create new table before fueling, false otherwise
     * @return {@link JetFuelRequest}
     */
    JetFuelRequest generateJetFuelRequest(final boolean isPartitioned, final Table table, final String tableColumnsAsString, final boolean isCompacted, final boolean dropTablePreFueling) {
        final JetFuelRequest request = new JetFuelRequest();
        request.addJetFuelQuery(HiveProperty.HIVE_MR_EXECUTION);
        request.addJetFuelQuery(HiveProperty.DYNAMIC_PARTITION_MODE);
        request.addJetFuelQuery(HiveProperty.DYNAMIC_PARTITION);

        if (jetFuelConfiguration.getMapReduceMemoryInMB() != null && jetFuelConfiguration.getMapReduceMemoryInMB() > 0) {
            request.addJetFuelQuery(new HiveProperty("mapreduce.map.memory.mb", jetFuelConfiguration.getMapReduceMemoryInMB()));
            request.addJetFuelQuery(new HiveProperty("mapreduce.reduce.memory.mb", jetFuelConfiguration.getMapReduceMemoryInMB()));
        }

        if (jetFuelConfiguration.getMapReduceJavaOptsInMB() != null && jetFuelConfiguration.getMapReduceJavaOptsInMB() > 0) {
            request.addJetFuelQuery(new HiveProperty("mapreduce.map.java.opts", String.format("-Xmx%sm", jetFuelConfiguration.getMapReduceJavaOptsInMB())));
            request.addJetFuelQuery(new HiveProperty("mapreduce.reduce.java.opts", String.format("-Xmx%sm", jetFuelConfiguration.getMapReduceJavaOptsInMB())));
        }

        if (jetFuelConfiguration.getParquetBlockSize() != null && jetFuelConfiguration.getParquetBlockSize() > 0) {
            request.addJetFuelQuery(new HiveProperty("parquet.block.size", jetFuelConfiguration.getParquetBlockSize()));
        }

        if (jetFuelConfiguration.getParquetPageSize() != null && jetFuelConfiguration.getParquetPageSize() > 0) {
            request.addJetFuelQuery(new HiveProperty("parquet.page.size", jetFuelConfiguration.getParquetPageSize()));
        }
        if (isCompacted) {
            request.addJetFuelQuery(new HiveProperty("mapred.max.split.size", jetFuelConfiguration.getMaxSplit()));
            request.addJetFuelQuery(new HiveProperty("mapred.min.split.size", jetFuelConfiguration.getMinSplit()));
            request.addJetFuelQuery(HiveProperty.MERGE_MAP_FILES);
            request.addJetFuelQuery(HiveProperty.MERGE_MAPRED_FILES);
            request.addJetFuelQuery(new HiveProperty("hive.merge.smallfiles.avgsize", jetFuelConfiguration.getSmallFileAvgSize()));
            request.addJetFuelQuery(new HiveProperty("hive.merge.size.per.task", jetFuelConfiguration.getSizePerTask()));
        }
        if (dropTablePreFueling) {
            request.addJetFuelQuery(getDropTableIfExists());
            request.addJetFuelQueries(fileFormatCompressor.getFileFormatCompressionQueries(jetFuelConfiguration));
        }
        log.info("Config queries {}", jetFuelConfiguration.getConfigQueries());
        if (jetFuelConfiguration.getConfigQueries() != null && !jetFuelConfiguration.getConfigQueries().isEmpty()) {
            for (final String query : jetFuelConfiguration.getConfigQueries()) {
                request.addJetFuelQuery(query);
            }
        }
        getInsertTableQuery(isPartitioned, table, tableColumnsAsString, request);

        return request;
    }

    /**
     * Retrieves drop table query
     *
     * @return drop table if exists query
     */
    private String getDropTableIfExists() {
        return String.format("DROP TABLE IF EXISTS %s.%s", jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());
    }

    /**
     * Retrieves insert table query
     *
     * @param isPartitioned true when table is partitioned, false otherwise
     */
    void getInsertTableQuery(final boolean isPartitioned, final Table table, final String tableColumnsAsString, final JetFuelRequest request) {
        Validate.notNull(table, "Table cannot be null");
        Validate.notBlank(tableColumnsAsString, "Table columns cannot be null/empty/blank");


        if (!isPartitioned) {
            request.addJetFuelQuery(String.format("INSERT OVERWRITE TABLE %s.%s SELECT * FROM %s.%s", jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable(),
                    jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable()));
            return;
        }

        final String partitions = hiveTableUtils.getPartitions(table);
        final StringBuilder insertQuery = new StringBuilder();
        insertQuery.append(String.format("INSERT OVERWRITE TABLE %s.%s PARTITION %s SELECT %s, %s FROM %s.%s", jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable(),
                partitions, tableColumnsAsString, partitions.replace("(", "").replace(")", ""), jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable()));

        switch (jetFuelConfiguration.getPartitionGroupingStrategy()) {

            case STATIC:
                getStaticGroupInsertTableQueries(insertQuery.toString(), request);
                return;

            case DYNAMIC:
                getDynamicGroupInsertTableQueries(insertQuery.toString(), request);
                return;

            case NONE:
            default:
                if (!isBlank(jetFuelConfiguration.getPartitionFilter())) {
                    insertQuery.append(String.format(" WHERE %s", jetFuelConfiguration.getPartitionFilter()));
                }

                request.addJetFuelQuery(insertQuery.toString());
        }
    }

    /**
     * Generates grouped insert partition table queries
     *
     * @param insertQuery base insert query
     * @param request     {@link JetFuelRequest}
     */
    private void getStaticGroupInsertTableQueries(final String insertQuery, final JetFuelRequest request) {

        if (isBlank(jetFuelConfiguration.getPartitionFilter())) {
            request.addJetFuelQuery(insertQuery);
            return;
        }

        final String[] partitionKeys = jetFuelConfiguration.getPartitionFilter().split("OR");
        if (partitionKeys.length == 1) {
            request.addJetFuelQuery(String.format("%s WHERE %s", insertQuery, jetFuelConfiguration.getPartitionFilter()));
            return;
        }

        log.info("Using static partitioning...");

        final List<List<String>> partitionGroups = Lists.partition(Arrays.asList(partitionKeys), jetFuelConfiguration.getInsertPartitionGroupSize().intValue());

        for (final List<String> partitionGroup : partitionGroups) {
            final String key = String.format("%s WHERE %s", insertQuery, String.join(" OR ", partitionGroup));
            final List<String> partitionQueries = new ArrayList<>();
            log.info("Query created for {} ", partitionGroup);

            // Create individual queries if group query fails
            for (final String partitionKey : partitionGroup) {
                final String query = String.format("%s WHERE %s", insertQuery, partitionKey);
                partitionQueries.add(query);
                log.info("Query created for inner key {} ", partitionKey);
            }
            request.addInsertPartitionQuery(key, partitionQueries);
        }

        log.info("Total partition query groups {}:", request.getInsertPartitionQueries().keySet().size());
    }

    /**
     * Generates dynamically grouped insert partition table queries
     *
     * @param insertQuery base insert query
     * @param request     {@link JetFuelRequest}
     */
    private void getDynamicGroupInsertTableQueries(final String insertQuery, final JetFuelRequest request) {

        if (isBlank(jetFuelConfiguration.getPartitionFilter())) {
            request.addJetFuelQuery(insertQuery);
            return;
        }

        final String[] partitionFilterFragments = jetFuelConfiguration.getPartitionFilter().split("OR");
        if (partitionFilterFragments.length == 1) {
            request.addJetFuelQuery(String.format("%s WHERE %s", insertQuery, jetFuelConfiguration.getPartitionFilter()));
            return;
        }

        log.info("Using dynamic partitioning...");

        request.setInsertPartitionTemplate(insertQuery);
        request.setPartitionGroupSize(jetFuelConfiguration.getInsertPartitionGroupSize());

        Stream.of(partitionFilterFragments)
                .map(String::trim)
                .forEach(request::addPartitionFilterFragment);
    }
}
