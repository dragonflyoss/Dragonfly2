/*
 *     Copyright 2020 The Dragonfly Authors
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
	"time"

	"github.com/docker/go-connections/tlsconfig"
	"github.com/go-sql-driver/mysql"
	drivermysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
)

func newMyqsl(cfg *config.Config) (*gorm.DB, error) {
	m := cfg.Database.Mysql

	// Format dsn string.
	dsn, err := formatMysqlDSN(m)
	if err != nil {
		return nil, err
	}

	// Connect to mysql.
	db, err := gorm.Open(drivermysql.Open(dsn), &gorm.Config{
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
	if m.Migrate {
		if err := migrate(db); err != nil {
			return nil, err
		}
	}

	// Run seed.
	if err := seed(cfg, db); err != nil {
		return nil, err
	}

	return db, nil
}

func formatMysqlDSN(cfg *config.MysqlConfig) (string, error) {
	mysqlCfg := mysql.Config{
		User:                 cfg.User,
		Passwd:               cfg.Password,
		Addr:                 fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Net:                  "tcp",
		DBName:               cfg.DBName,
		Loc:                  time.Local,
		AllowNativePasswords: true,
		ParseTime:            true,
		InterpolateParams:    true,
	}

	// Support TLS connection.
	if cfg.TLS != nil {
		mysqlCfg.TLSConfig = "custom"
		tls, err := tlsconfig.Client(tlsconfig.Options{
			CAFile:             cfg.TLS.CA,
			CertFile:           cfg.TLS.Cert,
			KeyFile:            cfg.TLS.Key,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		})
		if err != nil {
			return "", err
		}

		if err := mysql.RegisterTLSConfig("custom", tls); err != nil {
			return "", err
		}
	} else if cfg.TLSConfig != "" { // If no custom config is specified, use tlsConfig parameter if it is set.
		mysqlCfg.TLSConfig = cfg.TLSConfig
	}

	return mysqlCfg.FormatDSN(), nil
}
