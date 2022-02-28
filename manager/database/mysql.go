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

	"github.com/go-sql-driver/mysql"
	drivermysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
)

const (
	// Default number of cdn load limit
	defaultCDNLoadLimit = 300

	// Default number of client load limit
	defaultClientLoadLimit = 50
)

func newMyqsl(cfg *config.MysqlConfig) (*gorm.DB, error) {
	// Format dsn string
	dsn, err := formatDSN(cfg)
	if err != nil {
		return nil, err
	}

	// Connect to mysql
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

	// Run migration
	if cfg.Migrate {
		if err := migrate(db); err != nil {
			return nil, err
		}
	}

	// Run seed
	if err := seed(db); err != nil {
		return nil, err
	}

	return db, nil
}

func formatDSN(cfg *config.MysqlConfig) (string, error) {
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

	// Support TLS connection
	if cfg.TLS != nil {
		mysqlCfg.TLSConfig = "custom"
		tls, err := cfg.TLS.Client()
		if err != nil {
			return "", err
		}

		if err := mysql.RegisterTLSConfig("custom", tls); err != nil {
			return "", err
		}
	}

	return mysqlCfg.FormatDSN(), nil
}

func migrate(db *gorm.DB) error {
	return db.Set("gorm:table_options", "DEFAULT CHARSET=utf8mb4 ROW_FORMAT=Dynamic").AutoMigrate(
		&model.Job{},
		&model.CDNCluster{},
		&model.CDN{},
		&model.SchedulerCluster{},
		&model.Scheduler{},
		&model.SecurityRule{},
		&model.SecurityGroup{},
		&model.User{},
		&model.Oauth{},
		&model.Config{},
		&model.Application{},
	)
}

func seed(db *gorm.DB) error {
	var cdnClusterCount int64
	if err := db.Model(model.CDNCluster{}).Count(&cdnClusterCount).Error; err != nil {
		return err
	}
	if cdnClusterCount <= 0 {
		if err := db.Create(&model.CDNCluster{
			Model: model.Model{
				ID: uint(1),
			},
			Name: "cdn-cluster-1",
			Config: map[string]interface{}{
				"load_limit": defaultCDNLoadLimit,
			},
			IsDefault: true,
		}).Error; err != nil {
			return err
		}
	}

	var schedulerClusterCount int64
	if err := db.Model(model.SchedulerCluster{}).Count(&schedulerClusterCount).Error; err != nil {
		return err
	}
	if schedulerClusterCount <= 0 {
		if err := db.Create(&model.SchedulerCluster{
			Model: model.Model{
				ID: uint(1),
			},
			Name:   "scheduler-cluster-1",
			Config: map[string]interface{}{},
			ClientConfig: map[string]interface{}{
				"load_limit": defaultClientLoadLimit,
			},
			Scopes:    map[string]interface{}{},
			IsDefault: true,
		}).Error; err != nil {
			return err
		}
	}

	if schedulerClusterCount == 0 && cdnClusterCount == 0 {
		cdnCluster := model.CDNCluster{}
		if err := db.First(&cdnCluster).Error; err != nil {
			return err
		}

		schedulerCluster := model.SchedulerCluster{}
		if err := db.First(&schedulerCluster).Error; err != nil {
			return err
		}

		if err := db.Model(&cdnCluster).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
			return err
		}
	}

	return nil
}
