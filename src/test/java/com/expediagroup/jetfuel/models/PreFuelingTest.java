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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

/**
 * Tests for {@link PreFueling}
 */
public final class PreFuelingTest {

    @Test
    public void testIsDropTarget() {
        final PreFueling preFueling = new PreFueling();
        assertFalse(preFueling.isDropTarget());
    }

    @Test
    public void testSetDropTarget() {
        final PreFueling preFueling = new PreFueling();
        preFueling.setDropTarget(true);
        assertTrue(preFueling.isDropTarget());
    }

    @Test
    public void testEquals() {
        EqualsVerifier
                .forClass(PreFueling.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    @Test
    public void testToString() {
        final PreFueling preFueling = new PreFueling();
        assertNotNull(preFueling.toString());
        assertTrue(preFueling.toString().startsWith("PreFueling("));
    }

}
