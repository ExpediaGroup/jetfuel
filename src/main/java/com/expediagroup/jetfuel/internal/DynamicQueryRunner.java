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

import java.util.Deque;
import java.util.Stack;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.models.JetFuelRequest;

import lombok.extern.slf4j.Slf4j;

/**
 * Query Runner for Dynamic Partition Grouping
 */
@Slf4j
class DynamicQueryRunner extends QueryRunner {

    private static final double DYNAMIC_STEP_DIVISOR = 2.0;

    DynamicQueryRunner(final HiveDriverClient hiveDriverClient) {
        super(hiveDriverClient);
    }

    /**
     * Execute a JetFuel request.
     *
     * @param request {@link JetFuelRequest}
     */
    void execute(final JetFuelRequest request) {
        Validate.notNull(request, "Request cannot be null");
        try {
            hiveDriverClient.openConnection();

            request.getJetFuelQueries().forEach(hiveDriverClient::runQuery);

            // Start Dynamic Partition Inserts
            final String insertTemplate = request.getInsertPartitionTemplate();

            int successQueryCount = 0;
            int failedQueryCount = 0;

            // Remaining partitions initially contains all partitions
            // CurrentPartitions contains the group we're currently Fueling
            Stack<String> currentPartitions = new Stack<>();
            final Deque<String> remainingPartitions = request.getPartitionFilterFragments();
            final int partitionCount = request.getPartitionFilterFragments().size();

            // Initial group size based on configuration / partition count (whichever is smaller)
            long groupSize = request.getPartitionGroupSize();
            if (partitionCount < groupSize) {
                groupSize = partitionCount;
            }

            while (!remainingPartitions.isEmpty()) {

                // Ensure we have `groupSize` partitions in the current Stack
                while (currentPartitions.size() < groupSize && remainingPartitions.peek() != null) {
                    currentPartitions.push(remainingPartitions.pop());
                }

                // Generate current query
                final String currentQuery = String.format("%s WHERE %s", insertTemplate, String.join(" OR ", currentPartitions));

                try {
                    hiveDriverClient.runQuery(currentQuery);
                    log.info("Successfully executed insert partition grouped query");

                    successQueryCount++;
                    currentPartitions = new Stack<>();
                } catch (final Exception e) {
                    log.warn("Insert partition grouped query failed");
                    failedQueryCount++;

                    if (groupSize == 1) {
                        log.error("Failed for single partition {}", currentPartitions.peek());
                        log.error("Dynamic partition group size is already 1.");
                        throw new JetFuelException("Dynamic partition grouping unable to continue", e);
                    }

                    // Reduce the group size
                    // Use the actual count in case it's smaller than the configured group size
                    final long newGroupSize = (long) Math.ceil(currentPartitions.size() / DYNAMIC_STEP_DIVISOR);
                    log.warn("Reducing dynamic partition group size from {} to {}", groupSize, newGroupSize);
                    groupSize = newGroupSize;

                    // Restore current (failed) partitions to the remaining stack
                    while (currentPartitions.size() > groupSize) {
                        remainingPartitions.push(currentPartitions.pop());
                    }
                }
            }

            log.info("Final Dynamic Partition Group Size: {}", groupSize);
            log.info("Completed in {} successful queries", successQueryCount);
            log.info("Handled {} failed queries", failedQueryCount);

        } finally {
            hiveDriverClient.closeConnection();
        }
    }
}
