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
package com.expediagroup.jetfuel.internal;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Tests for {@link QueryRunnerFactory}
 */
public final class QueryRunnerFactoryTest {

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
    public void testCreate() throws ClassNotFoundException {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withPartitionGrouping("STATIC")
                .build();
        final HiveDriverClient hiveDriverClient = new HiveDriverClient(jetFuelConfiguration);
        final QueryRunner queryRunner = QueryRunnerFactory.create(jetFuelConfiguration, hiveDriverClient);
        assertTrue(queryRunner instanceof StaticQueryRunner);
    }

    @Test
    public void testCreateStatic() throws ClassNotFoundException {
        final JetFuelConfiguration jetFuelConfiguration = builder.build();
        final HiveDriverClient hiveDriverClient = new HiveDriverClient(jetFuelConfiguration);
        final QueryRunner queryRunner = QueryRunnerFactory.create(jetFuelConfiguration, hiveDriverClient);
        assertTrue(queryRunner instanceof StaticQueryRunner);
    }

    @Test
    public void testCreateDynamic() throws ClassNotFoundException {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withPartitionGrouping("DYNAMIC")
                .withEnablePartitionGrouping(true)
                .build();
        final HiveDriverClient hiveDriverClient = new HiveDriverClient(jetFuelConfiguration);
        final QueryRunner queryRunner = QueryRunnerFactory.create(jetFuelConfiguration, hiveDriverClient);
        assertTrue(queryRunner instanceof DynamicQueryRunner);
    }
}
