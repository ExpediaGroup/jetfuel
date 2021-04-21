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
import com.expediagroup.jetfuel.models.CompressionType;
import com.expediagroup.jetfuel.models.HiveProperty;

import lombok.extern.slf4j.Slf4j;

/**
 * Manager class to run JetFuel Queries
 */
@Slf4j
public class SessionPropertyCompressionStrategyImpl extends CompressionStrategy {

    private final String compressionPropertyName;

    public SessionPropertyCompressionStrategyImpl(final String compressionPropertyName) {
        this.compressionPropertyName = compressionPropertyName;
    }

    /**
     * {@inheritDoc}
     */
    public List<String> getCompressionQueries(final StringBuilder createTableQuery, final String targetCompression) throws JetFuelException {
        Validate.notNull(createTableQuery, "createTableQuery cannot be null");
        Validate.notBlank(targetCompression, "targetCompression cannot be null/blank/empty");

        final List<String> compressionQueries = new ArrayList<>();

        // Peform no compression when target compression type of UNCOMPRESSED is chosen
        if (CompressionType.UNCOMPRESSED.toString().equalsIgnoreCase(targetCompression)) {
            log.info("Skipping compression since compression type chosen is UNCOMPRESSED");
            compressionQueries.add(new HiveProperty("hive.exec.compress.output", "false").getQuery());
            compressionQueries.add(createTableQuery.toString());
            return compressionQueries;
        } else {
            // Add relevant hive session properties
            // Setting hive property output compress to true
            compressionQueries.add(new HiveProperty("hive.exec.compress.output", "true").getQuery());
            if (compressionPropertyName.contains("avro")) {
                // avro session property for compression
                compressionQueries.add(new HiveProperty("avro.output.codec", targetCompression.toLowerCase()).getQuery());
            } else if (compressionPropertyName.contains("text")) {
                // text session property for compression
                compressionQueries.add(new HiveProperty("mapreduce.output.fileoutputformat.compress", "true").getQuery());
                compressionQueries.add(new HiveProperty("mapreduce.output.fileoutputformat.compress.codec", String.format("org.apache.hadoop.io.compress.%s", CompressionType.getCompressionCodecByType(targetCompression.toUpperCase()))).getQuery());
            } else {
                // SEQ/RCFile session property for compression
                compressionQueries.add(new HiveProperty("mapred.output.compression.type", "BLOCK").getQuery());
                compressionQueries.add(new HiveProperty("mapred.output.compression.codec", String.format("org.apache.hadoop.io.compress.%s", CompressionType.getCompressionCodecByType(targetCompression.toUpperCase()))).getQuery());
                compressionQueries.add(new HiveProperty("io.compression.codecs", String.format("org.apache.hadoop.io.compress.%s", CompressionType.getCompressionCodecByType(targetCompression.toUpperCase()))).getQuery());
            }

            // Create table with relevant compression setting
            compressionQueries.add(getCreateTableQueryWithCompressionProperty(createTableQuery,
                    compressionPropertyName, targetCompression).toString());

            return compressionQueries;
        }
    }
}
