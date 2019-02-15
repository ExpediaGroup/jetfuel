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
package com.expediagroup.jetfuel.exception;

/**
 * Generic jetfuel exception base class.  This is an unchecked exception.
 */
public class JetFuelException extends RuntimeException {
    private static final long serialVersionUID = -1515582339940681860L;

    public JetFuelException(final String message) {
        super(message);
    }

    public JetFuelException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public JetFuelException(final Throwable cause) {
        super(cause);
    }
}
