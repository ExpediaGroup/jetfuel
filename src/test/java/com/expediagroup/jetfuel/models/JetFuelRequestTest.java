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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link JetFuelRequest}
 */
public final class JetFuelRequestTest {

    private final JetFuelRequest request = new JetFuelRequest();

    @Test(expected = IllegalArgumentException.class)
    public void testAddJetFuelQueryEmptyQuery() {
        request.addJetFuelQuery("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddJetFuelQueryBlankQuery() {
        request.addJetFuelQuery("   ");
    }

    @Test
    public void testAddJetFuelQuery() {
        request.addJetFuelQuery("query1");
        request.addJetFuelQuery("query2");

        assertEquals(2, request.getJetFuelQueries().size());
        assertTrue(request.getJetFuelQueries().contains("query1"));
        assertTrue(request.getJetFuelQueries().contains("query2"));
    }
}
