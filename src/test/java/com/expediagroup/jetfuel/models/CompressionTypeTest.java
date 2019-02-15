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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests for {@link CompressionType}
 */
public final class CompressionTypeTest {

    @Test
    public void testGetCompressionType() {
        assertEquals("SNAPPY", CompressionType.valueOf("SNAPPY").getCompressionType());
    }

    @Test
    public void testGetCompressionCodec() {
        assertEquals("SnappyCodec", CompressionType.valueOf("SNAPPY").getCompressionCodec());
    }

    @Test
    public void testGetCompressionCodecByType() {
        assertEquals("SnappyCodec", CompressionType.getCompressionCodecByType("SNAPPY"));
        assertEquals("GzipCodec", CompressionType.getCompressionCodecByType("GZIP"));
    }
}
