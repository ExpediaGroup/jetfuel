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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.models.CompressionStrategy;

import lombok.extern.slf4j.Slf4j;

/**
 * Manager class to run JetFuel Queries
 */
@Slf4j
public class TablePropertyCompressionStrategyImpl extends CompressionStrategy {
    // Valid table property name needed to be added for compression
    private final String compressionPropertyName;

    public TablePropertyCompressionStrategyImpl(final String compressionPropertyName) {
        this.compressionPropertyName = compressionPropertyName;
    }

    /**
     * {@inheritDoc}
     */
    public List<String> getCompressionQueries(final StringBuilder createTableQuery, final String targetCompression) throws JetFuelException {
        Validate.notNull(createTableQuery, "createTableQuery cannot be null");
        Validate.notBlank(targetCompression, "targetCompression cannot be null/blank/empty");

        final List<String> compressionQueries = new ArrayList<>();

        // Create table with relevant compression setting to the table properties
        compressionQueries.add(getCreateTableQueryWithCompressionProperty(createTableQuery,
                compressionPropertyName, targetCompression).toString());
        return compressionQueries;

    }
}
