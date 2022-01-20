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

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"moul.io/zapgorm2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
)

const (
	defaultCDNLoadLimit    = 300
	defaultClientLoadLimit = 100
)

func newMyqsl(cfg *config.MysqlConfig) (*gorm.DB, error) {
	logger := zapgorm2.New(logger.CoreLogger.Desugar())

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	dialector := mysql.Open(dsn)
	db, err := gorm.Open(dialector, &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger,
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

// TODO support mysql ssl
// Refer to https://github.com/dexidp/dex/blob/ff6e7c7688f363841f5cb8ffe12b41b990042f58/storage/ent/mysql.go
// https://github.com/cloudfoundry-incubator/notifications/blob/24e4d7cb633b7c14abe3797ebfadb7a1c539724e/application/mother.go
// https://stackoverflow.com/questions/52962028/how-to-create-ssl-connection-to-mysql-with-gorm
// https://pkg.go.dev/github.com/go-sql-driver/mysql#Config

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
