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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.DriverManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.JetFuelManager;
import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Tests for {@link JetFuelManagerImpl}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ JetFuelManagerImpl.class, HiveDriverClient.class, QueryRunnerFactory.class })
public final class JetFuelManagerImplTest {

    private final HiveTableUtils hiveTableUtils = mock(HiveTableUtils.class);
    private final QueryGenerator queryGenerator = mock(QueryGenerator.class);
    private final StaticQueryRunner queryRunner = mock(StaticQueryRunner.class);
    private final JetFuelConfiguration.Builder builder = new JetFuelConfiguration.Builder()
            .withSourceDatabase("sourceDb")
            .withSourceTable("sourceTable")
            .withTargetDatabase("targetDb")
            .withTargetTable("targetTable")
            .withHiveMetastoreUri("hiveMetastoreUri")
            .withHiveServer2Url("hiveUrl")
            .withHiveServer2Username("username")
            .withHiveServer2Password("password")
            .withTargetCompaction(false);

    @Before
    public void setup() {
        mockStatic(DriverManager.class);

        queryGenerator.getInsertTableQuery(anyBoolean(), any(), anyString(), any());
        doNothing().when(queryGenerator).getInsertTableQuery(anyBoolean(), any(), anyString(), any());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullJetFuelConfiguration() {
        new JetFuelManagerImpl(null, hiveTableUtils, queryGenerator, queryRunner);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullHiveConf() {
        new JetFuelManagerImpl(builder.build(), null, queryGenerator, queryRunner);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullQueryGenerator() {
        new JetFuelManagerImpl(builder.build(), hiveTableUtils, null, queryRunner);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullQueryRunner() {
        new JetFuelManagerImpl(builder.build(), hiveTableUtils, queryGenerator, null);
    }

    @Test
    public void testStart() {
        final JetFuelManager jetFuelManager = new JetFuelManagerImpl(builder.build(), hiveTableUtils, queryGenerator, queryRunner);
        jetFuelManager.fuel();
    }

    @Test
    public void testCompactionTrue() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetCompaction(true)
                .build();
        final JetFuelManager jetFuelManager = new JetFuelManagerImpl(jetFuelConfiguration, hiveTableUtils, queryGenerator, queryRunner);
        jetFuelManager.fuel();
    }

    @Test
    public void testCompactionFalse() {
        final JetFuelConfiguration jetFuelConfiguration = builder
                .withTargetCompaction(false)
                .build();
        final JetFuelManager jetFuelManager = new JetFuelManagerImpl(jetFuelConfiguration, hiveTableUtils, queryGenerator, queryRunner);
        jetFuelManager.fuel();
    }
}
