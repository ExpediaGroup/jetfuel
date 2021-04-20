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

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.models.JetFuelRequest;

/**
 * Tests for {@link DynamicQueryRunner}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DynamicQueryRunner.class, HiveDriverClient.class })
public final class DynamicQueryRunnerTest {

    private final HiveDriverClient client = mock(HiveDriverClient.class);
    private DynamicQueryRunner queryRunner;
    private JetFuelRequest request = new JetFuelRequest();

    @Before
    public void setup() {
        queryRunner = new DynamicQueryRunner(client);
        request.addJetFuelQuery("query1");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullHiveDriverClient() {
        new DynamicQueryRunner(null);
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
    public void testExecuteWithDynamicPartitionGrouping() {
        request.setInsertPartitionTemplate("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable");
        request.setPartitionGroupSize(5L);
        request.addPartitionFilterFragment("(trans_month = '2018-01')");
        request.addPartitionFilterFragment("(trans_month = '2018-02')");
        request.addPartitionFilterFragment("(trans_month = '2018-03')");
        request.addPartitionFilterFragment("(trans_month = '2018-04')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-03') OR (trans_month = '2018-04')");

        queryRunner.execute(request);

        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-03') OR (trans_month = '2018-04')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-03')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-04')");
        Mockito.verify(client).closeConnection();
    }

    @Test
    public void testExecuteWithDynamicPartitionGrouping2() {
        request.setInsertPartitionTemplate("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable");
        request.setPartitionGroupSize(6L);
        request.addPartitionFilterFragment("(trans_month = '2018-01')");
        request.addPartitionFilterFragment("(trans_month = '2018-02')");
        request.addPartitionFilterFragment("(trans_month = '2018-03')");
        request.addPartitionFilterFragment("(trans_month = '2018-04')");
        request.addPartitionFilterFragment("(trans_month = '2018-05')");
        request.addPartitionFilterFragment("(trans_month = '2018-06')");
        request.addPartitionFilterFragment("(trans_month = '2018-07')");
        request.addPartitionFilterFragment("(trans_month = '2018-08')");
        request.addPartitionFilterFragment("(trans_month = '2018-09')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06')");

        queryRunner.execute(request);

        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-04') OR (trans_month = '2018-05')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-06') OR (trans_month = '2018-07')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-08') OR (trans_month = '2018-09')");
        Mockito.verify(client).closeConnection();
    }

    @Test
    public void testExecuteWithDynamicPartitionGrouping3() {
        request.setInsertPartitionTemplate("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable");
        request.setPartitionGroupSize(7L);
        request.addPartitionFilterFragment("(trans_month = '2018-01')");
        request.addPartitionFilterFragment("(trans_month = '2018-02')");
        request.addPartitionFilterFragment("(trans_month = '2018-03')");
        request.addPartitionFilterFragment("(trans_month = '2018-04')");
        request.addPartitionFilterFragment("(trans_month = '2018-05')");
        request.addPartitionFilterFragment("(trans_month = '2018-06')");
        request.addPartitionFilterFragment("(trans_month = '2018-07')");
        request.addPartitionFilterFragment("(trans_month = '2018-08')");
        request.addPartitionFilterFragment("(trans_month = '2018-09')");
        request.addPartitionFilterFragment("(trans_month = '2018-10')");
        request.addPartitionFilterFragment("(trans_month = '2018-11')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06') OR (trans_month = '2018-07')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04')");

        queryRunner.execute(request);

        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04') OR (trans_month = '2018-05') OR (trans_month = '2018-06') OR (trans_month = '2018-07')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02') OR (trans_month = '2018-03') OR (trans_month = '2018-04')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-03') OR (trans_month = '2018-04')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-05') OR (trans_month = '2018-06')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-07') OR (trans_month = '2018-08')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-09') OR (trans_month = '2018-10')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-11')");
        Mockito.verify(client).closeConnection();
    }

    @Test(expected = JetFuelException.class)
    public void testExecuteWithAllFailed() {
        request.setInsertPartitionTemplate("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable");
        request.setPartitionGroupSize(2L);
        request.addPartitionFilterFragment("(trans_month = '2018-01')");
        request.addPartitionFilterFragment("(trans_month = '2018-02')");
        request.addPartitionFilterFragment("(trans_month = '2018-03')");
        request.addPartitionFilterFragment("(trans_month = '2018-04')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        doThrow(new IllegalArgumentException("Error"))
                .when(client)
                .runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01')");

        queryRunner.execute(request);

        Mockito.verify(client).openConnection();
        Mockito.verify(client, times(1)).runQuery("query1");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01') OR (trans_month = '2018-02')");
        Mockito.verify(client, times(1)).runQuery("INSERT OVERWRITE TABLE targetDb.targetTable PARTITION (partition1, partition2) SELECT cols, partition1, partition2 FROM sourceDb.sourceTable WHERE (trans_month = '2018-01')");
        Mockito.verify(client).closeConnection();
    }
}
