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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.yaml.snakeyaml.error.YAMLException;

import com.expediagroup.jetfuel.models.JetFuelConfiguration;

import lombok.extern.slf4j.Slf4j;

/**
 * Entry point for the application
 *
 * Parses the command-line arguments and starts JetFueling.
 */
@Slf4j
final class Application {
    public static void main(final String[] args) {

        final CommandLineParser commandLineParser = new DefaultParser();
        final Options options = new Options();
        options.addOption(Option.builder("yamlFile")
                .desc("YAML configuration file")
                .hasArg()
                .argName("FILE")
                .build());

        final CommandLine commandLine;
        try {
            commandLine = commandLineParser.parse(options, args);
        } catch (final ParseException e) {
            log.error("Unexpected error parsing command-line options", e);
            return;
        }

        if (commandLine.hasOption("yamlFile")) {

            final String yamlFile = commandLine.getOptionValue("yamlFile");
            final JetFuelConfiguration jetFuelConfiguration;
            try {
                jetFuelConfiguration = JetFuelConfiguration.loadFromYaml(yamlFile);
            } catch (final IOException e) {
                log.error("Unable to load YAML configuration file.", e);
                return;
            } catch (final YAMLException e) {
                log.error("Error parsing YAML file; is this a valid JetFuel configuration file?", e);
                return;
            }

            log.info("JetFuelConfiguration {}", jetFuelConfiguration);

            final JetFuelManager jetFuelManager = JetFuelManagerFactory.create(jetFuelConfiguration);
            jetFuelManager.fuel();

        } else {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar jetfuel.jar", options);
        }
    }
}
