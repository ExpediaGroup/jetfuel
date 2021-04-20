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
package com.expediagroup.jetfuel.models;


import java.util.List;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;

import lombok.extern.slf4j.Slf4j;

/**
 * Class to implement the relevant compression strategy and retrieve the compression queried for the file format
 */
@Slf4j
public abstract class FileFormatCompressor {

    /**
     * Generates a List of compression queries for the relevant file format
     *
     * @param jetFuelConfiguration JetFuelConfiguration
     * @return List of all compression queries
     * @throws JetFuelException thrown for any processing failure
     */
    public abstract List<String> getFileFormatCompressionQueries(JetFuelConfiguration jetFuelConfiguration) throws JetFuelException;

    /**
     * Generates a create table query for the relevant file format
     *
     * @param jetFuelConfiguration Jetfuel jetFuelConfiguration
     * @return create table query
     * @throws JetFuelException thrown for any processing failure
     */
    protected StringBuilder getCreateTableQuery(final JetFuelConfiguration jetFuelConfiguration) {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");

        if ("EMPTY".equalsIgnoreCase(jetFuelConfiguration.getTargetFileFormat().getCreateFormat())) {
            return new StringBuilder(String.format("CREATE TABLE %s.%s LIKE %s.%s",
                    jetFuelConfiguration.getTargetDatabase(),
                    jetFuelConfiguration.getTargetTable(), jetFuelConfiguration.getSourceDatabase(),
                    jetFuelConfiguration.getSourceTable()));
        } else {
            return new StringBuilder(String.format("CREATE TABLE %s.%s LIKE %s.%s STORED AS %s",
                    jetFuelConfiguration.getTargetDatabase(),
                    jetFuelConfiguration.getTargetTable(), jetFuelConfiguration.getSourceDatabase(),
                    jetFuelConfiguration.getSourceTable(), jetFuelConfiguration.getTargetFileFormat().getCreateFormat()));
        }
    }
}
