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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expediagroup.jetfuel.internal.JetFuelManagerImpl;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.FileFormat;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Tests for {@link Application}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ JetFuelManagerFactory.class, JetFuelManagerImpl.class, HiveTableUtils.class })
public final class ApplicationTest {

    private static final String usageText = "usage: java -jar jetfuel.jar\n -yamlFile <FILE>   YAML configuration file\n";

    private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private static final PrintStream originalOut = System.out;
    private static final PrintStream originalErr = System.err;

    @BeforeClass
    public static void beforeAll() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @AfterClass
    public static void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Before
    public void setUpStreams() {
        outContent.reset();
        errContent.reset();
    }

    @Test
    public void testMainNoArgs() {
        Application.main(new String[] { });
        assertEquals(usageText, outContent.toString());
    }

    @Test
    public void testMainUnexpectedArgs() {
        Application.main(new String[] { "-yaml" });
        assertTrue(outContent.toString().contains("Unrecognized option: -yaml"));
    }

    @Test
    public void testMainMissingFile() {
        Application.main(new String[] { "-yamlFile", "missing.yml" });
        assertTrue(outContent.toString().contains("Unable to load YAML configuration file."));
    }

    @Test
    public void testMainInvalidYaml() {
        Application.main(new String[] { "-yamlFile", "./src/test/resources/invalid.yml" });
        assertTrue(outContent.toString().contains("Error parsing YAML file"));
    }

    @Test
    public void testMainFueling() {
        final JetFuelManagerImpl mock = mock(JetFuelManagerImpl.class);
        mockStatic(JetFuelManagerFactory.class);
        when(JetFuelManagerFactory.create(any())).thenReturn(mock);

        Application.main(new String[] { "-yamlFile", "./src/test/resources/jetFuel.yml" });
        verify(mock, times(1)).fuel();

        final ArgumentCaptor<JetFuelConfiguration> configurationCaptor = ArgumentCaptor.forClass(JetFuelConfiguration.class);
        verifyStatic(JetFuelManagerFactory.class);
        JetFuelManagerFactory.create(configurationCaptor.capture());

        final JetFuelConfiguration jetFuelConfiguration = configurationCaptor.getValue();
        assertEquals("jetfuel_test", jetFuelConfiguration.getSourceDatabase());
        assertEquals("source_database", jetFuelConfiguration.getSourceTable());
        assertEquals("jetfuel_test", jetFuelConfiguration.getTargetDatabase());
        assertEquals("target_database", jetFuelConfiguration.getTargetTable());
        assertEquals(FileFormat.ORC, jetFuelConfiguration.getTargetFileFormat());
        assertEquals("SNAPPY", jetFuelConfiguration.getTargetCompression());
        assertTrue(jetFuelConfiguration.getTargetCompaction());
        assertNull(jetFuelConfiguration.getPartitionFilter());
        assertFalse(jetFuelConfiguration.isEnablePartitionGrouping());
    }
}
