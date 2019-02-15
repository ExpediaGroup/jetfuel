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
package com.expediagroup.jetfuel.internal;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.metastore.api.Table;

import com.expediagroup.jetfuel.JetFuelManager;
import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;
import com.expediagroup.jetfuel.models.JetFuelRequest;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation class for {@link JetFuelManager}
 */
@Slf4j
public class JetFuelManagerImpl implements JetFuelManager {

    private final JetFuelConfiguration jetFuelConfiguration;
    private final HiveTableUtils hiveTableUtils;
    private final QueryGenerator queryGenerator;
    private final QueryRunner queryRunner;

    /**
     * Constructor
     *
     * @param jetFuelConfiguration {@link JetFuelConfiguration}
     * @param hiveTableUtils {@link HiveTableUtils}
     */
    public JetFuelManagerImpl(final JetFuelConfiguration jetFuelConfiguration, final HiveTableUtils hiveTableUtils,
                              final QueryGenerator queryGenerator, final QueryRunner queryRunner) {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");
        Validate.notNull(hiveTableUtils, "hiveTableUtils cannot be null");
        Validate.notNull(queryGenerator, "queryGenerator cannot be null");
        Validate.notNull(queryRunner, "queryRunner cannot be null");

        this.jetFuelConfiguration = jetFuelConfiguration;
        this.hiveTableUtils = hiveTableUtils;
        this.queryGenerator = queryGenerator;
        this.queryRunner = queryRunner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fuel() throws JetFuelException {

        log.info("---------- PLANNING ----------");
        log.info("Started fueling for source table {}.{} and target table {}.{}", jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable(),
                jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());

        final Table sourceTable = hiveTableUtils.getTable(jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable());
        Table targetTable = null;
        try {
            targetTable = hiveTableUtils.getTable(jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());
            log.info("Target table {}.{} exists", jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());
        } catch (final JetFuelException e) {
            log.error("Unable to retrieve table {}.{} from the Hive Metastore.  Please verify the table exists and the Hive Metastore configuration is correct.",
                    jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());
        }

        final boolean isTablePartitioned = hiveTableUtils.isPartitioned(sourceTable);
        log.info("Table {}.{} is partitioned: {}", jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable(), isTablePartitioned);

        final String tableColumnsAsString = hiveTableUtils.getTableColumnsAsString(sourceTable);

        final boolean isCompacted = jetFuelConfiguration.getTargetCompaction();

        final boolean dropTablePreFueling = getDropTablePreFueling(targetTable);
        log.info("Drop table pre fueling {}", dropTablePreFueling);

        // Generate Queries
        final JetFuelRequest request = queryGenerator.generateJetFuelRequest(isTablePartitioned, sourceTable, tableColumnsAsString, isCompacted, dropTablePreFueling);

        // Execute Queries
        log.info("---------- EXECUTING ----------");
        queryRunner.execute(request);

        log.info("Finished fueling for source table {}.{} and target table {}.{}", jetFuelConfiguration.getSourceDatabase(), jetFuelConfiguration.getSourceTable(),
                jetFuelConfiguration.getTargetDatabase(), jetFuelConfiguration.getTargetTable());
    }

    /**
     * Determines if the drop needs to be dropped and created before fueling.
     * @param table {@link Table}
     * @return true if table does not exist.
     *              if prefueling config is not provided and table does not exist.
     *              if table exists and prefueling drop target is true
     *        false if prefueling config is not provided and table exists.
     *              if table exists and prefueling drop target is false.
     */
    private boolean getDropTablePreFueling(final Table table) {
        if (table == null) {
            return true;
        }

        if (jetFuelConfiguration.getPreFueling() == null) {
            return false;
        }

        return jetFuelConfiguration.getPreFueling().isDropTarget();
    }
}
