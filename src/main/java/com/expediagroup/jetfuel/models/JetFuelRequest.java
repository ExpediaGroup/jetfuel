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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.commons.lang3.Validate;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public final class JetFuelRequest {
    /**
     * Ordered sequence of Hive Queries to be run.
     */
    private final List<String> jetFuelQueries = new ArrayList<>();

    /**
     * Set of grouped INSERT PARTITION queries (for static partitioning)
     */
    private final SetMultimap<String, String> insertPartitionQueries = LinkedHashMultimap.create();

    /**
     * Stack of partition filter fragments (for dynamic partitioning)
     */
    private final Deque<String> partitionFilterFragments = new ArrayDeque<>();

    /**
     * INSERT PARTITION query template (for dynamic partitioning)
     */
    @Setter
    private String insertPartitionTemplate;

    @Setter
    private long partitionGroupSize;

    /**
     * Adds a Hive query to run during Fueling
     *
     * @param query Hive query
     */
    public void addJetFuelQuery(final String query) {
        Validate.notBlank(query, "Query cannot be null/empty/blank");
        log.info("Query generated {}", query);
        jetFuelQueries.add(query);
    }

    /**
     * Adds a list of Hive queries to run during Fueling
     *
     * @param queries Hive queries list
     */
    public void addJetFuelQueries(final List<String> queries) {
        for (final String query : queries) {
            log.info("Query generated {}", query);
            jetFuelQueries.add(query);
        }
    }

    /**
     * Adds a Hive Property to set during Fueling
     *
     * @param property Hive Property instance
     */
    public void addJetFuelQuery(final HiveProperty property) {
        Validate.notNull(property, "HiveProperty cannot be null/empty/blank");
        final String query = property.getQuery();
        log.info("Query generated {}", query);
        jetFuelQueries.add(query);
    }

    /**
     * Adds a Partition Key Query group (Static Partition Grouping)
     *
     * @param queryKey   Hive query that INSERTs a group of partitions
     * @param queryValue List of Hive queries that INSERT each partition individually
     */
    public void addInsertPartitionQuery(final String queryKey, final List<String> queryValue) {
        Validate.notBlank(queryKey, "QueryKey cannot be null/empty/blank");
        Validate.notNull(queryValue, "QueryValue cannot be null");
        log.info("Query generated {}", queryKey);
        insertPartitionQueries.putAll(queryKey, queryValue);
    }

    /**
     * Adds a PartitionFilterFragment (Dynamic Partition Grouping)
     * (e.g. "(partitionKey = 'value')")
     *
     * @param partitionFilter Fragment value
     */
    public void addPartitionFilterFragment(final String partitionFilter) {
        Validate.notBlank(partitionFilter, "PartitionFilter cannot be null/empty/blank");
        partitionFilterFragments.addLast(partitionFilter);
    }
}
