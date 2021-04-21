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
package com.expediagroup.jetfuel;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.JetFuelManagerImpl;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.FileFormat;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Tests for {@link JetFuelManagerFactory}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ JetFuelManagerFactory.class, JetFuelManager.class, JetFuelManagerImpl.class })
public final class JetFuelManagerFactoryTest {

    private final JetFuelConfiguration jetFuelConfiguration = new JetFuelConfiguration.Builder()
            .withSourceDatabase("sourceDb")
            .withSourceTable("sourceTable")
            .withTargetDatabase("targetDb")
            .withTargetTable("targetTable")
            .withHiveMetastoreUri("hiveMetastoreUri")
            .withHiveServer2Url("hiveUrl")
            .withHiveServer2Username("username")
            .withHiveServer2Password("password")
            .withTargetFileFormat(FileFormat.ORC)
            .withTargetCompaction(false)
            .build();

    private final HiveTableUtils hiveTableUtils = mock(HiveTableUtils.class);

    @Test(expected = NullPointerException.class)
    public void testCreateNullJetFuelConfiguration() {
        JetFuelManagerFactory.create(null);
    }

    @Test
    public void testCreate() throws Exception {
        whenNew(JetFuelManagerImpl.class).withArguments(any(), any(), any(), any()).thenReturn(mock(JetFuelManagerImpl.class));
        whenNew(HiveTableUtils.class).withAnyArguments().thenReturn(hiveTableUtils);

        final JetFuelManager jetFuelManager = JetFuelManagerFactory.create(jetFuelConfiguration);
        assertNotNull(jetFuelManager);
        assertTrue(jetFuelManager instanceof JetFuelManagerImpl);
    }

    @Test(expected = JetFuelException.class)
    public void testConstructorMetaException() throws Exception {
        whenNew(HiveTableUtils.class).withAnyArguments().thenReturn(hiveTableUtils);
        whenNew(JetFuelManagerImpl.class).withArguments(any(), any(), any(), any()).thenThrow(new MetaException());
        JetFuelManagerFactory.create(jetFuelConfiguration);
    }
}
