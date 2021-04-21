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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * File Formats supported by JetFuel
 */
@Slf4j
@Getter
public enum FileFormat {

    TEXT("TEXTFILE", "orc.compress", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.GZIP.toString(),
            CompressionType.SNAPPY.toString()))),
    SEQ("SEQUENCEFILE", "orc.compress", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.GZIP.toString(),
            CompressionType.SNAPPY.toString()))),
    ORC("ORC", "orc.compress", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.NONE.toString(),
            CompressionType.ZLIB.toString(),
            CompressionType.SNAPPY.toString()))),
    RC("RCFILE", "orc.compress", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.GZIP.toString(),
            CompressionType.SNAPPY.toString()))),
    AVRO("AVRO", "orc.compress", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.SNAPPY.toString()))),
    PARQUET("PARQUET", "parquet.compression", new HashSet<>(Arrays.asList(
            CompressionType.UNCOMPRESSED.toString(),
            CompressionType.GZIP.toString(),
            CompressionType.SNAPPY.toString()))),
    NULL("EMPTY", "empty", new HashSet<>());

    private final String createFormat;
    private final String compressionTblPropertyName;
    private final Set<String> validCompressions;

    /**
     * Constructor
     *
     * @param createFormat               create format for file format
     * @param compressionTblPropertyName compress format for file format
     */
    FileFormat(final String createFormat, final String compressionTblPropertyName, final Set<String> validCompressions) {
        this.createFormat = createFormat;
        this.compressionTblPropertyName = compressionTblPropertyName;
        this.validCompressions = validCompressions;
    }
}
