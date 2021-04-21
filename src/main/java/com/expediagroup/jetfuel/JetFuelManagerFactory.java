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

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.expediagroup.jetfuel.exception.JetFuelException;
import com.expediagroup.jetfuel.internal.JetFuelManagerImpl;
import com.expediagroup.jetfuel.internal.QueryGenerator;
import com.expediagroup.jetfuel.internal.QueryGeneratorFactory;
import com.expediagroup.jetfuel.internal.QueryRunner;
import com.expediagroup.jetfuel.internal.QueryRunnerFactory;
import com.expediagroup.jetfuel.internal.hive.HiveDriverClient;
import com.expediagroup.jetfuel.internal.hive.HiveTableUtils;
import com.expediagroup.jetfuel.models.JetFuelConfiguration;

/**
 * Factory class for {@link JetFuelManager}
 */
public class JetFuelManagerFactory {

    /**
     * Creates a new instance of a JetFuelManager.
     *
     * @param jetFuelConfiguration {@link JetFuelConfiguration}
     * @return new instance
     */
    public static JetFuelManager create(final JetFuelConfiguration jetFuelConfiguration) {
        Validate.notNull(jetFuelConfiguration, "jetFuelConfiguration cannot be null");

        try {
            final HiveTableUtils hiveTableUtils = createHiveTableUtils(jetFuelConfiguration);
            final QueryGenerator queryGenerator = QueryGeneratorFactory.create(jetFuelConfiguration, hiveTableUtils);
            final QueryRunner queryRunner = QueryRunnerFactory.create(jetFuelConfiguration, new HiveDriverClient(jetFuelConfiguration));

            return new JetFuelManagerImpl(jetFuelConfiguration, hiveTableUtils, queryGenerator, queryRunner);
        } catch (final MetaException | ClassNotFoundException e) {
            throw new JetFuelException(e);
        }
    }

    private static HiveTableUtils createHiveTableUtils(final JetFuelConfiguration jetFuelConfiguration) throws MetaException {
        final HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, jetFuelConfiguration.getHiveMetastoreUri());

        return new HiveTableUtils(hiveConf);
    }
}
