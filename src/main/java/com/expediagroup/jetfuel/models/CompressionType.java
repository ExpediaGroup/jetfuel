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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Compression types supported by JetFuel
 */
@Slf4j
@Getter
public enum CompressionType {

    SNAPPY("SNAPPY", "SnappyCodec"),
    ZLIB("ZLIB", "NA"),
    GZIP("GZIP", "GzipCodec"),
    NONE("NONE", "NA"),
    UNCOMPRESSED("UNCOMPRESSED", "NA");

    private static final Map<String, String> typeMapping = Collections.unmodifiableMap(initializeMapping());
    private final String compressionType;
    private final String compressionCodec;

    /**
     * Constructor
     *
     * @param compressionType  the compression type
     * @param compressionCodec the relevant codec for the compression type
     */
    CompressionType(final String compressionType, final String compressionCodec) {
        this.compressionType = compressionType;
        this.compressionCodec = compressionCodec;
    }

    /**
     * Method to initialize the map of compression types and their codecs
     *
     * @return map of compression types and their codecs
     */
    private static Map<String, String> initializeMapping() {
        final Map<String, String> mMap = new HashMap<>();
        for (final CompressionType compressionType : CompressionType.values()) {
            mMap.put(compressionType.compressionType, compressionType.compressionCodec);
        }
        return mMap;
    }

    /**
     * Method to extract the compression codec of a given compression type
     *
     * @param compressionType the compression type
     * @return compression codec
     */
    public static String getCompressionCodecByType(final String compressionType) {
        return typeMapping.get(compressionType);
    }

    @Override
    public String toString() {
        return compressionType;
    }
}
