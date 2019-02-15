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
package com.expediagroup.jetfuel.internal.formats;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.TablePropertyCompressionStrategyImpl;
import com.expediagroup.jetfuel.models.CompressionStrategy;
import com.expediagroup.jetfuel.models.FileFormat;
import com.expediagroup.jetfuel.models.FileFormatCompressor;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

import lombok.extern.slf4j.Slf4j;

/**
 * Class to implement the relevant compression strategy and retrieve the compression queried for the file format
 */
@Slf4j
public class OrcFileFormatCompressorImpl extends FileFormatCompressor {

    private static final Set<String> ORC_COMPRESS_OPTIONS = FileFormat.ORC.getValidCompressions();

    private final CompressionStrategy compressionStrategy;

    public OrcFileFormatCompressorImpl() {
        compressionStrategy = new TablePropertyCompressionStrategyImpl("orc.compress");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getFileFormatCompressionQueries(final JetFuelConfiguration jetFuelConfiguration) throws JetFuelException {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        // Add the create table query string
        final StringBuilder createTableQuery = getCreateTableQuery(jetFuelConfiguration);
        String targetCompression = jetFuelConfiguration.getTargetCompression().toUpperCase();

        // Reject invalid/unsupported compression settings
        if (!ORC_COMPRESS_OPTIONS.contains(jetFuelConfiguration.getTargetCompression().toUpperCase())) {
            final String errorMessage = String.format("Provided compression option - %s is not supported for ORC File Format", jetFuelConfiguration.getTargetCompression());
            log.error(errorMessage);
            throw new JetFuelException(errorMessage);
        }
        // Set compression to NONE type when no compression is chosen
        if ("UNCOMPRESSED".equals(targetCompression) || "NONE".equals(targetCompression)) {
            log.info("Skipping compression since compression type chosen is UNCOMPRESSED");
            targetCompression = "NONE";
        }
        // Returns a list containing compression queries
        return compressionStrategy.getCompressionQueries(createTableQuery, targetCompression);
    }
}
