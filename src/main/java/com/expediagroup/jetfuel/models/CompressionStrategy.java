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
package com.expediagroup.jetfuel.models;


import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.google.common.collect.Maps;

/**
 * Class implements the compression strategy utilized for extracting compression queries
 */
public abstract class CompressionStrategy {

    /**
     * Retrieves a list of compression queries for a given table and target compression
     *
     * @param createTableQuery  create table query
     * @param targetCompression target compression setting for file format
     * @return List of compression queries
     * @throws JetFuelException thrown for any processing failure
     */
    public abstract List<String> getCompressionQueries(final StringBuilder createTableQuery, final String targetCompression) throws JetFuelException;

    /**
     * Build the tblproperties clause of a create table statement
     *
     * @param properties a key/value map of table properties
     * @return A formatted string of table properties
     */
    private String getTblProperties(final Map<String, String> properties) {
        Validate.notNull(properties, "properties cannot be null");

        if (properties.isEmpty()) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("tblProperties(");
        int count = 0;
        for (final String key : properties.keySet()) {
            final String value = properties.get(key);

            if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
                continue;
            }

            sb.append(String.format("\"%s\"=\"%s\"", key, value));

            count++;
            if (count < properties.size()) {
                sb.append(",");
            }
        }

        sb.append(")");

        return "tblProperties()".equals(sb.toString()) ? "" : sb.toString();
    }

    /**
     * Build the createTableQuery with the relevant compression property setting
     *
     * @param createTableQuery        create table query
     * @param compressionPropertyName compression property name for the file format
     * @param targetCompression       targetCompression for the file format
     * @return A formatted string of table properties
     */
    protected StringBuilder getCreateTableQueryWithCompressionProperty(final StringBuilder createTableQuery, final String compressionPropertyName, final String targetCompression) {
        Validate.notNull(createTableQuery, "createTableQuery cannot be null");
        Validate.notBlank(targetCompression, "compressionPropertyName cannot be null/blank/empty");
        Validate.notBlank(targetCompression, "targetCompression cannot be null/blank/empty");

        final Map<String, String> properties = Maps.newHashMap();
        properties.put(compressionPropertyName, targetCompression);
        final String tblProperties = getTblProperties(properties);
        createTableQuery.append(" ");
        createTableQuery.append(tblProperties);
        return createTableQuery;
    }
}
