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
package com.expediagroup.jetfuel;


import com.expediagroup.jetfuel.exception.JetFuelException;

/**
 * Manager class to perform JetFuel operations
 */
public interface JetFuelManager {

    /**
     * Starts JetFueling.
     *
     * @throws JetFuelException thrown for any processing failure
     */
    void fuel() throws JetFuelException;
}
