package database

import (
	"fmt"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Database struct {
	db    *gorm.DB
	rdb   *redis.Client
	cache *cache.Cache
}

func New(cfg *config.Config) (*Database, error) {
	db, err := NewMyqsl(cfg.Mysql)
	if err != nil {
		return nil, err
	}

	rdb, err := NewRedis(cfg.Redis)
	if err != nil {
		return nil, err
	}

	return &Database{
		db:    db,
		rdb:   rdb,
		cache: NewCache(cfg.Cache, rdb),
	}, nil
}

func NewMyqsl(cfg *config.MysqlConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

	dialector := mysql.Open(dsn)

	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(
		&models.CDN{},
		&models.CDNInstance{},
		&models.Scheduler{},
		&models.SchedulerInstance{},
		&models.SecurityGroup{},
	); err != nil {
		return nil, err
	}

	return db, nil
}

func NewRedis(cfg *config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return rdb, nil
}

func NewCache(cfg *config.CacheConfig, rdb *redis.Client) *cache.Cache {
	return cache.New(&cache.Options{
		Redis:      rdb,
		LocalCache: cache.NewTinyLFU(cfg.Size, cfg.TTL),
	})
}
