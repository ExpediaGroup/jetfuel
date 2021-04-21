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
package com.expediagroup.jetfuel.internal.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

import lombok.extern.slf4j.Slf4j;

/**
 * Hive Driver Client to run Hive JDBC queries
 */
@Slf4j
public class HiveDriverClient {

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private final JetFuelConfiguration jetFuelConfiguration;
    private Connection connection;

    /**
     * Constructor
     *
     * @param jetFuelConfiguration {@link JetFuelConfiguration}
     * @throws ClassNotFoundException Thrown if the Hive driver cannot be loaded
     */
    public HiveDriverClient(final JetFuelConfiguration jetFuelConfiguration) throws ClassNotFoundException {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        this.jetFuelConfiguration = jetFuelConfiguration;

        Class.forName(DRIVER_NAME);
    }

    /**
     * Runs Hive query.
     *
     * @param query Hive query
     * @throws JetFuelException thrown if any exception happens while running the query
     */
    public void runQuery(final String query) throws JetFuelException {
        Validate.notBlank(query, "Query cannot be null/empty/blank");

        if (connection == null) {
            throw new JetFuelException("Connection must be opened prior to running queries.");
        }

        final Instant start = Instant.now();

        try (final Statement statement = connection.createStatement()) {

            log.info("Running query {}", query);
            statement.execute(query);

            log.info("Query Successful ({})", Formatter.formatDuration(Duration.between(start, Instant.now())));
        } catch (final Exception e) {
            final String errorMessage = String.format("Query Failed (%s): %s",
                    Formatter.formatDuration(Duration.between(start, Instant.now())),
                    e.getMessage());
            log.info(errorMessage);
            throw new JetFuelException(errorMessage, e);
        }
    }

    /**
     * Open Hive JDBC connection.
     */
    public void openConnection() throws JetFuelException {
        try {
            if (connection == null) {
                connection = DriverManager.getConnection(jetFuelConfiguration.getHiveServer2Url(), jetFuelConfiguration.getHiveServer2Username(), jetFuelConfiguration.getHiveServer2Password());
            }
        } catch (final Exception e) {
            final String errorMessage = String.format("Error opening connection: %s ", e.getMessage());
            throw new JetFuelException(errorMessage, e);
        }
    }

    /**
     * Close Hive JDBC connection.
     */
    public void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connection = null;
            }
        } catch (final Exception e) {
            final String errorMessage = String.format("Error closing connection: %s ", e.getMessage());
            log.warn(errorMessage);
        }
    }
}
