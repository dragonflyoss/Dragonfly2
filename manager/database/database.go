package database

import (
	"fmt"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Database struct {
	*gorm.DB
}

func New(cfg *config.MysqlConfig) (*Database, error) {
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

	return &Database{db}, nil
}
