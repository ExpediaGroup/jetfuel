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
package com.expediagroup.jetfuel.internal;

import org.apache.commons.lang3.Validate;

import com.expediagroup.jetfuel.internal.formats.AvroFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.DefaultFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.OrcFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.ParquetFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.RcFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.SeqFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.formats.TextFileFormatCompressorImpl;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.FileFormatCompressor;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Factory class for {@link QueryGenerator}.
 *
 * Different QueryGenerators are available with different strategies for Fueling.
 */
public final class QueryGeneratorFactory {

    public static QueryGenerator create(final JetFuelConfiguration jetFuelConfiguration, final HiveTableUtils hiveTableUtils) {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        Validate.notNull(hiveTableUtils, "hiveTableUtils cannot be null");

        final FileFormatCompressor fileFormatCompressor = getFileFormatCompressor(jetFuelConfiguration.getTargetFileFormat().getCreateFormat());

        return new QueryGenerator(hiveTableUtils, jetFuelConfiguration, fileFormatCompressor);
    }

    private static FileFormatCompressor getFileFormatCompressor (final String createFormat) {
        switch (createFormat) {
            case "TEXTFILE":
                return new TextFileFormatCompressorImpl();
            case "SEQUENCEFILE":
                return new SeqFileFormatCompressorImpl();
            case "ORC":
                return new OrcFileFormatCompressorImpl();
            case "RCFILE":
                return new RcFileFormatCompressorImpl();
            case "AVRO":
                return new AvroFileFormatCompressorImpl();
            case "PARQUET":
                return new ParquetFileFormatCompressorImpl();
            default:
                // case when file format provided is null/blank/empty
                return new DefaultFileFormatCompressorImpl();
        }
    }
}
