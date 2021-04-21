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
package com.expediagroup.jetfuel.internal.hive;

import static org.junit.Assert.assertEquals;

import java.time.Duration;

import org.junit.Test;


/**
 * Tests for {@link Formatter}
 */
public final class FormatterTest {

    @Test
    public void testFormatDurationSeconds() {
        final Duration duration = Duration.ofSeconds(1L);
        assertEquals("00:01", Formatter.formatDuration(duration));
    }

    @Test
    public void testFormatDurationMinutes() {
        final Duration duration = Duration.ofSeconds(61);
        assertEquals("01:01", Formatter.formatDuration(duration));
    }

    @Test
    public void testFormatDurationMinutes2() {
        final Duration duration = Duration.ofSeconds(300);
        assertEquals("05:00", Formatter.formatDuration(duration));
    }

    @Test
    public void testFormatDurationHours() {
        final Duration duration = Duration.ofMinutes(75);
        assertEquals("75:00", Formatter.formatDuration(duration));
    }
}
