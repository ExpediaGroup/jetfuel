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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration options for JetFuel.
 *
 * Immutable.  Use the JetFuelConfiguration.Builder to build new instances.
 */
@Data
@Slf4j
public final class JetFuelConfiguration {

    private static final String CONF_PATH = "config.properties";

    private final String sourceTable;
    private final String sourceDatabase;
    private final String targetTable;
    private final String targetDatabase;
    private final FileFormat targetFileFormat;
    private final String targetCompression;
    private final Boolean targetCompaction;
    private final String hiveMetastoreUri;
    private final String hiveServer2Url;
    private final String hiveServer2Username;
    private final String hiveServer2Password;
    private final String partitionFilter;
    private final String maxSplit;
    private final String minSplit;
    private final String mapReduceTaskTimeout;

    private final String smallFileAvgSize;
    private final String sizePerTask;
    private final Long mapReduceMemoryInMB;
    private final Long mapReduceJavaOptsInMB;
    private final Long parquetBlockSize;
    private final Long parquetPageSize;

    /**
     * Determines the size of partition groupings (if needed).
     * If {@link #partitionGroupingStrategy} is set to DYNAMIC, this will be used as the starting value.
     * @since 0.5.0
     */
    private final Long insertPartitionGroupSize;

    /**
     * Enables partition grouping.
     *
     * @deprecated Replaced by {@link #enablePartitionGrouping} and will be removed in a future release.
     */
    @Deprecated
    private final boolean groupPartitionOverride;

    /**
     * Enables partition grouping.
     *
     * @since 0.6.0
     */
    private final boolean enablePartitionGrouping;

    /**
     * Determines which strategy to use for partition grouping (if enabled)
     *
     * Contains an enum version corresponding to the Builder field partitionGrouping.
     *
     * @since 0.6.0
     */
    private final PartitionGrouping partitionGroupingStrategy;

    /**
     * List of additional Hive queries to be executed (optional)
     */
    private final List<String> configQueries;

    /**
     * Contains additional options for the pre-fueling phase (optional)
     */
    private final PreFueling preFueling;

    private JetFuelConfiguration(final Builder builder) {
        sourceTable = builder.sourceTable;
        sourceDatabase = builder.sourceDatabase;
        targetTable = builder.targetTable;
        targetDatabase = builder.targetDatabase;
        targetFileFormat = builder.targetFileFormat;
        targetCompression = builder.targetCompression;
        targetCompaction = builder.targetCompaction;
        hiveMetastoreUri = builder.hiveMetastoreUri;
        hiveServer2Url = builder.hiveServer2Url;
        hiveServer2Username = builder.hiveServer2Username;
        hiveServer2Password = builder.hiveServer2Password;
        partitionFilter = builder.partitionFilter;
        maxSplit = builder.maxSplit;
        minSplit = builder.minSplit;
        smallFileAvgSize = builder.smallFileAvgSize;
        sizePerTask = builder.sizePerTask;
        mapReduceMemoryInMB = builder.mapReduceMemoryInMB;
        mapReduceJavaOptsInMB = builder.mapReduceJavaOptsInMB;
        parquetBlockSize = builder.parquetBlockSize;
        parquetPageSize = builder.parquetPageSize;
        insertPartitionGroupSize = builder.insertPartitionGroupSize;
        groupPartitionOverride = builder.groupPartitionOverride;
        enablePartitionGrouping = builder.enablePartitionGrouping;
        partitionGroupingStrategy = builder.partitionGrouping;
        mapReduceTaskTimeout = builder.mapReduceTaskTimeout;
        configQueries = builder.configQueries == null
                ? null
                : ImmutableList.copyOf(builder.configQueries);
        preFueling = builder.preFueling;
    }

    /**
     * Deserialize a YAML file into an instance of this class.
     *
     * @param yamlFileName YAML file name
     * @return Instance of {@link JetFuelConfiguration}
     * @throws IOException thrown when unable to load the input YAML file
     */
    public static JetFuelConfiguration loadFromYaml(final String yamlFileName) throws IOException {
        log.info("Reading YAML File {}", yamlFileName);
        try (final InputStream stream = new FileInputStream(yamlFileName);
             final Reader file = new InputStreamReader(stream, Charsets.UTF_8)) {

            final Representer representer = new Representer();
            representer.getPropertyUtils().setSkipMissingProperties(true);

            final Yaml yaml = new Yaml(representer);
            final Builder builder = yaml.loadAs(file, Builder.class);

            return builder.build();
        }
    }

    /**
     * Builder class for the immutable {@link JetFuelConfiguration}
     */
    @NoArgsConstructor
    public static final class Builder {
        public String sourceTable;
        public String sourceDatabase;
        public String targetTable;
        public String targetDatabase;
        protected FileFormat targetFileFormat;
        public String targetCompression;
        public Boolean targetCompaction;
        public String hiveMetastoreUri;
        public String hiveServer2Url;
        public String hiveServer2Username;
        public String hiveServer2Password;
        public String partitionFilter;
        public String maxSplit;
        public String minSplit;
        public String smallFileAvgSize;
        public String sizePerTask;
        public String mapReduceTaskTimeout;
        public Long mapReduceMemoryInMB;
        public Long mapReduceJavaOptsInMB;
        public Long parquetBlockSize;
        public Long parquetPageSize;
        public Long insertPartitionGroupSize;
        @Deprecated
        public boolean groupPartitionOverride;
        public boolean enablePartitionGrouping;
        protected PartitionGrouping partitionGrouping;
        public List<String> configQueries;
        public PreFueling preFueling;

        private Builder(final Builder builder) {
            sourceTable = builder.sourceTable;
            sourceDatabase = builder.sourceDatabase;
            targetTable = builder.targetTable;
            targetDatabase = builder.targetDatabase;
            targetFileFormat = builder.targetFileFormat;
            targetCompression = builder.targetCompression;
            targetCompaction = builder.targetCompaction;
            hiveMetastoreUri = builder.hiveMetastoreUri;
            hiveServer2Url = builder.hiveServer2Url;
            hiveServer2Username = builder.hiveServer2Username;
            hiveServer2Password = builder.hiveServer2Password;
            partitionFilter = builder.partitionFilter;
            maxSplit = builder.maxSplit;
            minSplit = builder.minSplit;
            smallFileAvgSize = builder.smallFileAvgSize;
            sizePerTask = builder.sizePerTask;
            mapReduceMemoryInMB = builder.mapReduceMemoryInMB;
            mapReduceJavaOptsInMB = builder.mapReduceJavaOptsInMB;
            parquetBlockSize = builder.parquetBlockSize;
            parquetPageSize = builder.parquetPageSize;
            insertPartitionGroupSize = builder.insertPartitionGroupSize;
            groupPartitionOverride = builder.groupPartitionOverride;
            enablePartitionGrouping = builder.enablePartitionGrouping;
            partitionGrouping = builder.partitionGrouping;
            configQueries = builder.configQueries;
            preFueling = builder.preFueling;
            mapReduceTaskTimeout = builder.mapReduceTaskTimeout;
        }

        public JetFuelConfiguration build() {
            validate();
            return new JetFuelConfiguration(this);
        }

        private void validate() {

            setConfigSettings();

            Validate.notBlank(sourceDatabase, "sourceDatabase cannot be null or blank");
            Validate.notBlank(sourceTable, "sourceTable cannot be null or blank");

            Validate.notBlank(targetDatabase, "targetDatabase cannot be null or blank");
            Validate.notBlank(targetTable, "targetTable cannot be null or blank");

            if (StringUtils.equalsIgnoreCase(sourceDatabase, targetDatabase) && StringUtils.equalsIgnoreCase(sourceTable, targetTable)) {
                throw new IllegalArgumentException("Source and target database/table name cannot be same");
            }
            Validate.notBlank(hiveMetastoreUri, "hiveMetastoreUri cannot be null or blank");
            Validate.notBlank(hiveServer2Url, "hiveServer2Url cannot be null or blank");
            Validate.notNull(hiveServer2Username, "hiveServer2Username cannot be null");

            // Set to Uncompressed if no target compression is provided
            if (isBlank(targetCompression)) {
                targetCompression = CompressionType.UNCOMPRESSED.toString();
            }

            // Set password to empty string if read from YML as null. This makes the password field null-safe.
            hiveServer2Password = isBlank(hiveServer2Password) ? "" : hiveServer2Password;
        }

        private void setConfigSettings() {
            final PropertiesConfiguration config;

            try {
                config = new PropertiesConfiguration(CONF_PATH);
            } catch (final ConfigurationException e) {
                throw new JetFuelException("Unable to load configurations", e);
            }

            if (targetCompaction == null) {
                targetCompaction = false;
            }
            if (targetCompaction) {
                maxSplit = isBlank(maxSplit) ? config.getString("maxSplit") : maxSplit;
                minSplit = isBlank(minSplit) ? config.getString("minSplit") : minSplit;
                smallFileAvgSize = isBlank(smallFileAvgSize) ? config.getString("smallFileAvgSize") : smallFileAvgSize;
                sizePerTask = isBlank(sizePerTask) ? config.getString("sizePerTask") : sizePerTask;

                Validate.notBlank(maxSplit, "maxSplit cannot be null");
                Validate.notBlank(minSplit, "minSplit cannot be null");
                Validate.notBlank(sizePerTask, "sizePerTask cannot be null");
                Validate.notBlank(smallFileAvgSize, "smallFileAvgSize cannot be null");
            }

            insertPartitionGroupSize = insertPartitionGroupSize == null || insertPartitionGroupSize < 1 ? config.getLong("insertPartitionGroupSize") : insertPartitionGroupSize;

            // Map deprecated field
            if (groupPartitionOverride) {
                enablePartitionGrouping = true;
            }

            // Always group PARQUET queries
            if (targetFileFormat == FileFormat.PARQUET) {
                enablePartitionGrouping = true;
            }

            // PartitionGrouping should be non-null
            if (partitionGrouping == null) {
                partitionGrouping = PartitionGrouping.NONE;
            }

            // Use Static strategy by default
            if (enablePartitionGrouping) {
                if (partitionGrouping == PartitionGrouping.NONE) {
                    partitionGrouping = PartitionGrouping.STATIC;
                }
            } else {
                partitionGrouping = PartitionGrouping.NONE;
            }
            // Add mapReduceTaskTimeout for map-reduce jobs
            mapReduceTaskTimeout = isBlank(mapReduceTaskTimeout) ? config.getString("mapReduceTaskTimeout") : mapReduceTaskTimeout;
        }

        //
        // set Methods -- set values and return
        // Required by SnakeYAML
        // Only non-standard setters needed
        //

        public void setTargetFileFormat(final String targetFileFormat) {
            try {
                this.targetFileFormat = isBlank(targetFileFormat) ? FileFormat.valueOf("NULL") : FileFormat.valueOf(targetFileFormat.toUpperCase());
            } catch (final Exception e) {
                final String errorMessage = String.format("Wrong fileFormat provided. Supported file formats %s",
                        Arrays.stream(FileFormat.values()).map(FileFormat::toString).collect(Collectors.joining(", ")));
                throw new JetFuelException(errorMessage, e);
            }
        }

        public void setPartitionGrouping(final String partitionGrouping) {
            try {
                this.partitionGrouping = isBlank(partitionGrouping) ? PartitionGrouping.NONE : PartitionGrouping.valueOf(partitionGrouping.toUpperCase());
            } catch (final Exception e) {
                throw new JetFuelException("Unrecognized partitionGrouping provided.", e);
            }
        }

        //
        // with Methods -- modify and return a new Builder
        //

        public Builder withSourceTable(final String sourceTable) {
            this.sourceTable = sourceTable;
            return new Builder(this);
        }

        public Builder withSourceDatabase(final String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
            return new Builder(this);
        }

        public Builder withTargetTable(final String targetTable) {
            this.targetTable = targetTable;
            return new Builder(this);
        }

        public Builder withTargetDatabase(final String targetDatabase) {
            this.targetDatabase = targetDatabase;
            return new Builder(this);
        }

        public Builder withTargetFileFormat(final FileFormat targetFileFormat) {
            this.targetFileFormat = targetFileFormat;
            return new Builder(this);
        }

        public Builder withTargetFileFormat(final String targetFileFormat) {
            setTargetFileFormat(targetFileFormat);
            return new Builder(this);
        }

        public Builder withTargetCompression(final String targetCompression) {
            this.targetCompression = targetCompression;
            return new Builder(this);
        }

        public Builder withTargetCompaction(final Boolean targetCompaction) {
            this.targetCompaction = targetCompaction;
            return new Builder(this);
        }

        public Builder withHiveMetastoreUri(final String hiveMetastoreUri) {
            this.hiveMetastoreUri = hiveMetastoreUri;
            return new Builder(this);
        }

        public Builder withHiveServer2Url(final String hiveServer2Url) {
            this.hiveServer2Url = hiveServer2Url;
            return new Builder(this);
        }

        public Builder withHiveServer2Username(final String hiveServer2Username) {
            this.hiveServer2Username = hiveServer2Username;
            return new Builder(this);
        }

        public Builder withHiveServer2Password(final String hiveServer2Password) {
            this.hiveServer2Password = hiveServer2Password;
            return new Builder(this);
        }

        public Builder withPartitionFilter(final String partitionFilter) {
            this.partitionFilter = partitionFilter;
            return new Builder(this);
        }

        public Builder withMaxSplit(final String maxSplit) {
            this.maxSplit = maxSplit;
            return new Builder(this);
        }

        public Builder withMinSplit(final String minSplit) {
            this.minSplit = minSplit;
            return new Builder(this);
        }

        public Builder withSmallFileAvgSize(final String smallFileAvgSize) {
            this.smallFileAvgSize = smallFileAvgSize;
            return new Builder(this);
        }

        public Builder withSizePerTask(final String sizePerTask) {
            this.sizePerTask = sizePerTask;
            return new Builder(this);
        }

        public Builder withMapReduceMemoryInMB(final Long mapReduceMemoryInMB) {
            this.mapReduceMemoryInMB = mapReduceMemoryInMB;
            return new Builder(this);
        }

        public Builder withMapReduceJavaOptsInMB(final Long mapReduceJavaOptsInMB) {
            this.mapReduceJavaOptsInMB = mapReduceJavaOptsInMB;
            return new Builder(this);
        }

        public Builder withParquetBlockSize(final Long parquetBlockSize) {
            this.parquetBlockSize = parquetBlockSize;
            return new Builder(this);
        }

        public Builder withParquetPageSize(final Long parquetPageSize) {
            this.parquetPageSize = parquetPageSize;
            return new Builder(this);
        }

        public Builder withInsertPartitionGroupSize(final Long insertPartitionGroupSize) {
            this.insertPartitionGroupSize = insertPartitionGroupSize;
            return new Builder(this);
        }

        public Builder withGroupPartitionOverride(final boolean groupPartitionOverride) {
            this.groupPartitionOverride = groupPartitionOverride;
            return new Builder(this);
        }

        public Builder withEnablePartitionGrouping(final boolean enablePartitionGrouping) {
            this.enablePartitionGrouping = enablePartitionGrouping;
            return new Builder(this);
        }

        public Builder withPartitionGrouping(final String partitionGrouping) {
            setPartitionGrouping(partitionGrouping);
            return new Builder(this);
        }

        public Builder withPartitionGrouping(final PartitionGrouping partitionGrouping) {
            this.partitionGrouping = partitionGrouping;
            return new Builder(this);
        }

        public Builder withConfigQueries(final List<String> configQueries) {
            this.configQueries = configQueries;
            return new Builder(this);
        }

        public Builder withPreFueling(final PreFueling preFueling) {
            this.preFueling = preFueling;
            return new Builder(this);
        }

        public Builder withMapReduceTaskTimeout(final String mapReduceTaskTimeout) {
            this.mapReduceTaskTimeout = mapReduceTaskTimeout;
            return new Builder(this);
        }
    }
}
