/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package database

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
)

func newPostgres(cfg *config.Config) (*gorm.DB, error) {
	postgresCfg := &cfg.Database.Postgres

	// Format dsn string.
	dsn := formatPostgresDSN(postgresCfg)

	// Connect to postgres.
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: cfg.Database.Postgres.PreferSimpleProtocol,
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   zapgorm2.New(logger.CoreLogger.Desugar()),
	})
	if err != nil {
		return nil, err
	}

	// Run migration.
	if postgresCfg.Migrate {
		if err := migrate(db); err != nil {
			return nil, err
		}
	}

	// Run seed.
	if err := seed(db); err != nil {
		return nil, err
	}

	return db, nil
}

func formatPostgresDSN(cfg *config.PostgresConfig) string {
	return fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v sslmode=%v TimeZone=%v",
		cfg.Host,
		cfg.User,
		cfg.Password,
		cfg.DBName,
		cfg.Port,
		cfg.SSLMode,
		cfg.Timezone,
	)
}
