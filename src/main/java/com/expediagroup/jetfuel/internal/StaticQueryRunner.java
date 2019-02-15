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

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.models.JetFuelRequest;

import lombok.extern.slf4j.Slf4j;

/**
 * Manager class to run JetFuel Queries
 */
@Slf4j
class StaticQueryRunner extends QueryRunner {

    StaticQueryRunner(final HiveDriverClient hiveDriverClient) {
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

            // Partition Grouping
            if (!request.getInsertPartitionQueries().isEmpty()) {
                // Static Partition Grouping
                request.getInsertPartitionQueries().keySet().forEach(query -> {
                    try {
                        hiveDriverClient.runQuery(query);
                        log.info("Successfully executed insert partition grouped query");
                    } catch (final Exception e) {
                        log.warn("Insert partition grouped query failed, trying individual partitions for group");
                        request.getInsertPartitionQueries().get(query).forEach(individualQuery -> {
                            log.info("Executing individual partition query");
                            hiveDriverClient.runQuery(individualQuery);
                            log.info("Successfully executed individual partition query");
                        });
                    }
                });
            }

        } finally {
            hiveDriverClient.closeConnection();
        }
    }

}
