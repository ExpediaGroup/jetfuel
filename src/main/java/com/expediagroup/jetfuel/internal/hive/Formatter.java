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
package com.expediagroup.jetfuel.internal.hive;

import java.time.Duration;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Custom formatter methods
 */
final class Formatter {

    private Formatter() {
    }

    /**
     * Formats a Duration using "mm:ss" format.
     *
     * Examples:
     *   PT1M1S returns "01:01"
     *   PT1H15M returns "75:00"
     *
     * @param duration Duration instance
     * @return String containing the formatted value
     */
    static String formatDuration(final Duration duration) {
        Validate.notNull(duration, "duration cannot be null");
        final long millis = duration.toMillis();
        return DurationFormatUtils.formatDuration(millis, "mm:ss", true);
    }
}
