package database

import (
	"fmt"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
	"github.com/go-redis/redis/v8"
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
	if err := db.AutoMigrate(
		&model.CDNCluster{},
		&model.CDN{},
		&model.SchedulerCluster{},
		&model.Scheduler{},
		&model.SecurityGroup{},
	); err != nil {
		return nil, err
	}

	return db, nil
}

func NewRedis(cfg *config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}
