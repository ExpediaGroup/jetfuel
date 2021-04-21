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
package com.expediagroup.jetfuel.internal.formats;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.SessionPropertyCompressionStrategyImpl;
import com.expediagroup.jetfuel.models.CompressionStrategy;
import com.expediagroup.jetfuel.models.FileFormat;
import com.expediagroup.jetfuel.models.FileFormatCompressor;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

import lombok.extern.slf4j.Slf4j;

/**
 * Class to implement the relevant compression strategy and retrieve the compression queried for the file format
 */
@Slf4j
public class SeqFileFormatCompressorImpl extends FileFormatCompressor {

    private static final Set<String> SEQ_COMPRESS_OPTIONS = FileFormat.SEQ.getValidCompressions();

    private final CompressionStrategy compressionStrategy;

    public SeqFileFormatCompressorImpl() {
        compressionStrategy = new SessionPropertyCompressionStrategyImpl("seq.compress");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getFileFormatCompressionQueries(final JetFuelConfiguration jetFuelConfiguration) throws JetFuelException {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");

        final StringBuilder createTableQuery = getCreateTableQuery(jetFuelConfiguration);
        final String targetCompression = jetFuelConfiguration.getTargetCompression().toUpperCase();

        // Reject invalid/unsupported compression settings
        if (!SEQ_COMPRESS_OPTIONS.contains(targetCompression)) {
            final String errorMessage = String.format("Provided compression option - %s is not supported for SEQ File Format", targetCompression);
            log.error(errorMessage);
            throw new JetFuelException(errorMessage);
        }

        // Returns a list containing just the create table
        return compressionStrategy.getCompressionQueries(createTableQuery, targetCompression);
    }
}
