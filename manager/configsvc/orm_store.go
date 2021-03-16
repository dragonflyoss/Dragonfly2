package configsvc

import (
	"context"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sort"
	"time"
)

type ormStore struct {
	db    *gorm.DB
	table string
}

type OrmConfig struct {
	ID        string         `gorm:"primary_key"`
	Object    string         `gorm:"size:255"`
	Type      string         `gorm:"size:255"`
	Version   uint64         `gorm:"index:order_version"`
	Data      []byte         `gorm:"data"`
	CreatedAt time.Time      `gorm:"create_at"`
	UpdatedAt time.Time      `gorm:"update_at"`
	Deleted   gorm.DeletedAt `gorm:"index"`
}

func ormConfig2InnerConfig(config *OrmConfig) *Config {
	return &Config{
		ID:       config.ID,
		Object:   config.Object,
		Type:     config.Type,
		Version:  config.Version,
		Data:     config.Data,
		CreateAt: config.CreatedAt,
		UpdateAt: config.UpdatedAt,
	}
}

func innerConfig2OrmConfig(config *Config) *OrmConfig {
	now := time.Now()
	return &OrmConfig{
		ID:        config.ID,
		Object:    config.Object,
		Type:      config.Type,
		Version:   config.Version,
		Data:      config.Data,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func NewOrmStore(cfg *config.StoreConfig) (Store, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Mysql.User, cfg.Mysql.Password, cfg.Mysql.IP, cfg.Mysql.Port, cfg.Mysql.Db)

	if db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		orm := &ormStore{
			db:    db,
			table: cfg.Mysql.Table,
		}

		if err := orm.withTable().AutoMigrate(&OrmConfig{}); err != nil {
			return nil, err
		} else {
			return orm, nil
		}
	}
}

func (orm *ormStore) withTable() (tx *gorm.DB) {
	if orm.table != "" {
		return orm.db.Table(orm.table)
	} else {
		return orm.db
	}
}

func (orm *ormStore) AddConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	cfg := innerConfig2OrmConfig(config)
	cfg.ID = id

	tx := orm.withTable().Create(cfg)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add config error: %s", tx.Error.Error())
	} else {
		return ormConfig2InnerConfig(cfg), nil
	}
}

func (orm *ormStore) DeleteConfig(ctx context.Context, id string) (*Config, error) {
	cfg := &OrmConfig{
		ID: id,
	}

	tx := orm.withTable().Delete(cfg)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete config error: %s", err.Error())
	} else {
		return ormConfig2InnerConfig(cfg), nil
	}
}

func (orm *ormStore) UpdateConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	cfg := innerConfig2OrmConfig(config)
	cfg.ID = id

	tx := orm.withTable().Updates(cfg)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "update config error: %s", err.Error())
	} else if err != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update config error: %s", err.Error())
	} else {
		return ormConfig2InnerConfig(cfg), nil
	}
}

func (orm *ormStore) GetConfig(ctx context.Context, id string) (*Config, error) {
	cfg := &OrmConfig{
		ID: id,
	}

	tx := orm.withTable().Find(cfg)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "get config error: %s", err.Error())
	} else if err != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get config error: %s", err.Error())
	} else {
		return ormConfig2InnerConfig(cfg), nil
	}
}

func (orm *ormStore) ListConfigs(ctx context.Context, object string) ([]*Config, error) {
	return orm.listSortedConfig(ctx, object, 0)
}

func (orm *ormStore) LatestConfig(ctx context.Context, object string, objType string) (*Config, error) {
	configs, err := orm.listSortedConfig(ctx, object, 1)
	if err != nil {
		return nil, err
	} else {
		return configs[0], nil
	}
}

func (orm *ormStore) listSortedConfig(ctx context.Context, object string, maxLen uint32) ([]*Config, error) {
	var ormConfigs []OrmConfig
	configs := make([]*Config, 0)

	tx := orm.withTable().Where("object = ?", object).Find(&ormConfigs)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "list sorted config error %s, object %s, maxLen %d%s", object, maxLen, err.Error())
	} else if err != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list sorted config error %s, object %s, maxLen %d", object, maxLen, err.Error())
	} else {
		for _, cfg := range ormConfigs {
			configs = append(configs, ormConfig2InnerConfig(&cfg))
		}
	}

	if len(configs) <= 0 {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "list sorted config empty, object %s, maxLen %d", object, maxLen)
	}

	sort.Sort(SortConfig(configs))
	len := uint32(len(configs))
	if maxLen == 0 || len <= maxLen {
		return configs, nil
	} else {
		return configs[0:maxLen], nil
	}
}
