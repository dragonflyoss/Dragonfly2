package database

import (
	"fmt"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
	"github.com/go-redis/redis/v8"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Database struct {
	DB  *gorm.DB
	RDB *redis.Client
}

func New(cfg *config.Config) (*Database, error) {
	db, err := newMyqsl(cfg.Database.Mysql)
	if err != nil {
		return nil, err
	}

	return &Database{
		DB:  db,
		RDB: NewRedis(cfg.Database.Redis),
	}, nil
}

func NewRedis(cfg *config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.CacheDB,
	})
}

func newMyqsl(cfg *config.MysqlConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	dialector := mysql.Open(dsn)
	db, err := gorm.Open(dialector, &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		return nil, err
	}

	// Run migration
	if err := migrate(db); err != nil {
		return nil, err
	}

	// Run seed
	if err := seed(db); err != nil {
		return nil, err
	}

	return db, nil
}

func migrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&model.CDNCluster{},
		&model.CDN{},
		&model.SchedulerCluster{},
		&model.Scheduler{},
		&model.SecurityGroup{},
		&model.User{},
	)
}

func seed(db *gorm.DB) error {
	var cdnClusterCount int64
	if err := db.Model(model.CDNCluster{}).Count(&cdnClusterCount).Error; err != nil {
		return err
	}
	if cdnClusterCount <= 0 {
		if err := db.Create(&model.CDNCluster{
			Name:   "cdn-cluster-1",
			Config: map[string]interface{}{},
		}).Error; err != nil {
			return err
		}
	}

	var adminUserCount int64
	var adminUserName = "admin"
	if err := db.Model(model.User{}).Where("name = ?", adminUserName).Count(&adminUserCount).Error; err != nil {
		return err
	}
	if adminUserCount <= 0 {
		encryptedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte("Dragonfly2"), bcrypt.MinCost)
		if err != nil {
			return err
		}
		if err := db.Create(&model.User{
			EncryptedPassword: string(encryptedPasswordBytes),
			Name:              adminUserName,
			Email:             fmt.Sprintf("%s@Dragonfly2.com", adminUserName),
			State:             model.UserStateEnabled,
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
			Name:         "scheduler-cluster-1",
			Config:       map[string]interface{}{},
			ClientConfig: map[string]interface{}{},
			IsDefault:    true,
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
