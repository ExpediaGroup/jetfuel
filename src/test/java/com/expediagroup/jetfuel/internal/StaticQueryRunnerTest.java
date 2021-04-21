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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.models.JetFuelRequest;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link StaticQueryRunner}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ StaticQueryRunner.class, HiveDriverClient.class })
public final class StaticQueryRunnerTest {

    private final HiveDriverClient client = mock(HiveDriverClient.class);
    private StaticQueryRunner queryRunner;
    private JetFuelRequest request = new JetFuelRequest();

    @Before
    public void setup() {
        queryRunner = new StaticQueryRunner(client);
        request.addJetFuelQuery("query1");
    }

    @Test(expected = NullPointerException.class)
    public void testContructorNullHiveDriverClient() {
        new StaticQueryRunner(null);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteNullRequest() {
        queryRunner.execute(null);
    }

    @Test
    public void testExecuteNoQueries() {
        request = new JetFuelRequest();
        queryRunner.execute(request);
        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(0)).runQuery(anyString());
        Mockito.verify(client).closeConnection();
    }

    @Test
    public void testExecute() {
        queryRunner.execute(request);
        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client).closeConnection();
    }

    @Test
    public void testExecuteWithInsertGroupQueries() {
        request.addInsertPartitionQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')",
                ImmutableList.of(
                        "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01')",
                        "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-02')"));
        queryRunner.execute(request);
        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        Mockito.verify(client).closeConnection();
    }

    @Test
    public void testExecuteWithInsertGroupFailureQueries() {
        request.addInsertPartitionQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')",
                ImmutableList.of(
                        "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01')",
                        "INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-02')"));
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        queryRunner.execute(request);

        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-02')");
        Mockito.verify(client).closeConnection();
    }
}
