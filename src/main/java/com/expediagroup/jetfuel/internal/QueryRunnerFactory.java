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
import com.expediagroup.jetfuel.models.JetFuelConfiguration;
import com.expediagroup.jetfuel.models.PartitionGrouping;

/**
 * Factory class for {@link QueryRunner}.
 *
 * Different QueryRunners are available with different strategies for Fueling.
 */
public final class QueryRunnerFactory {

    public static QueryRunner create(final JetFuelConfiguration jetFuelConfiguration, final HiveDriverClient hiveDriverClient) {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        Validate.notNull(hiveDriverClient, "hiveDriverClient cannot be null");

        if (jetFuelConfiguration.isEnablePartitionGrouping() && jetFuelConfiguration.getPartitionGroupingStrategy() == PartitionGrouping.DYNAMIC) {
            return new DynamicQueryRunner(hiveDriverClient);
        }

        return new StaticQueryRunner(hiveDriverClient);
    }
}
