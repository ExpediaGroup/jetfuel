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
package com.expediagroup.jetfuel.internal.hive;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Tests for {@link HiveDriverClient}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ HiveDriverClient.class, DriverManager.class })
public final class HiveDriverClientTest {

    private final Connection connection = mock(Connection.class);
    private final Statement statement = mock(Statement.class);
    private final JetFuelConfiguration jetFuelConfiguration = new JetFuelConfiguration.Builder()
            .withSourceDatabase("sourceDb")
            .withSourceTable("sourceTable")
            .withTargetDatabase("targetDb")
            .withTargetTable("targetTable")
            .withHiveMetastoreUri("hiveMetastoreUri")
            .withHiveServer2Url("url")
            .withHiveServer2Username("user")
            .withHiveServer2Password("password")
            .build();

    private HiveDriverClient hiveDriverClient;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(connection.isClosed()).thenReturn(false);
        hiveDriverClient = new HiveDriverClient(jetFuelConfiguration);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullHiveServe2Url() throws ClassNotFoundException {
        new HiveDriverClient(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRunQueryNullQuery() {
        hiveDriverClient.runQuery(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRunQueryEmptyQuery() {
        hiveDriverClient.runQuery("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRunQueryBlankQuery() {
        hiveDriverClient.runQuery("  ");
    }

    @Test
    public void testOpenConnection() {
        hiveDriverClient.openConnection();
    }

    @Test
    public void testOpenConnectionTwice() throws SQLException {
        hiveDriverClient.openConnection();
        hiveDriverClient.openConnection();

        verifyStatic(DriverManager.class);
        DriverManager.getConnection(anyString(), anyString(), anyString());
    }

    @Test
    public void testRunQuery() {
        hiveDriverClient.openConnection();
        hiveDriverClient.runQuery("query");
    }

    @Test(expected = JetFuelException.class)
    public void testRunQueryError() throws SQLException {
        when(statement.execute(anyString())).thenThrow(IllegalArgumentException.class);
        hiveDriverClient.runQuery("query");
    }

    @Test(expected = JetFuelException.class)
    public void testRunQueryNoConnection() {
        hiveDriverClient.runQuery("query");
    }

    @Test
    public void testCloseConnection() throws SQLException {
        hiveDriverClient.openConnection();
        hiveDriverClient.closeConnection();
        verify(connection, times(1)).close();
    }

    @Test
    public void testCloseConnectionError() throws SQLException {
        when(connection.isClosed()).thenThrow(IllegalArgumentException.class);
        hiveDriverClient.closeConnection();
    }

    @Test
    public void testCloseConnectionNope() {
        hiveDriverClient.closeConnection();
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testCloseConnectionTwice() throws SQLException {
        hiveDriverClient.openConnection();
        hiveDriverClient.closeConnection();
        hiveDriverClient.closeConnection();

        verify(connection, times(1)).close();
    }
}
