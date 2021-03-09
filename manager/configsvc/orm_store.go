package configsvc

import (
	"context"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sort"
	"time"
)

type ormStore struct {
	db *gorm.DB
}

type OrmConfig struct {
	ID        string    `gorm:"primary_key"`
	Object    string    `gorm:"size:255"`
	ObjType   string    `gorm:"size:255"`
	Version   uint64    `gorm:"index:order_version"`
	Body      []byte    `gorm:"body"`
	CreatedAt time.Time `gorm:"-"`
	UpdatedAt time.Time `gorm:"-"`
}

func NewOrmStore(cfg *config.StoreConfig) (Store, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Mysql.Username, cfg.Mysql.Password, cfg.Mysql.IP, cfg.Mysql.Port, cfg.Mysql.DbName)

	if db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		if err := db.AutoMigrate(&OrmConfig{}); err != nil {
			return nil, err
		} else {
			return &ormStore{db: db}, nil
		}
	}
}

func (orm *ormStore) AddConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	now := time.Now()
	cfg := &OrmConfig{
		ID:        id,
		Object:    config.Object,
		ObjType:   config.ObjType,
		Version:   config.Version,
		Body:      config.Body,
		CreatedAt: now,
		UpdatedAt: now,
	}

	tx := orm.db.Create(cfg)

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "add config error: %s", tx.Error.Error())
	} else {
		return &Config{
			ID:      cfg.ID,
			Object:  cfg.Object,
			ObjType: cfg.ObjType,
			Version: cfg.Version,
			Body:    cfg.Body,
		}, nil
	}
}

func (orm *ormStore) DeleteConfig(ctx context.Context, id string) (*Config, error) {
	cfg := &OrmConfig{
		ID: id,
	}

	tx := orm.db.Delete(cfg)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "delete config error: %s", tx.Error.Error())
	} else {
		return &Config{
			ID:      cfg.ID,
			Object:  cfg.Object,
			ObjType: cfg.ObjType,
			Version: cfg.Version,
			Body:    cfg.Body,
		}, nil
	}
}

func (orm *ormStore) UpdateConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	cfg := OrmConfig{
		ID:      id,
		Object:  config.Object,
		ObjType: config.ObjType,
		Version: config.Version,
		Body:    config.Body,
	}

	tx := orm.db.Updates(cfg)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "update config error: %s", tx.Error.Error())
	} else {
		return &Config{
			ID:      cfg.ID,
			Object:  cfg.Object,
			ObjType: cfg.ObjType,
			Version: cfg.Version,
			Body:    cfg.Body,
		}, nil
	}
}

func (orm *ormStore) GetConfig(ctx context.Context, id string) (*Config, error) {
	cfg := &OrmConfig{
		ID: id,
	}

	tx := orm.db.Find(cfg)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "get config error: %s", tx.Error.Error())
	} else {
		return &Config{
			ID:      id,
			Object:  cfg.Object,
			ObjType: cfg.ObjType,
			Version: cfg.Version,
			Body:    cfg.Body,
		}, nil
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

	tx := orm.db.Where("object = ?", object).Find(&ormConfigs)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "list sorted config error: %s", tx.Error.Error())
	} else {
		for _, cfg := range ormConfigs {
			configs = append(configs, &Config{
				ID:      cfg.ID,
				Object:  cfg.Object,
				ObjType: cfg.ObjType,
				Version: cfg.Version,
				Body:    cfg.Body,
			})
		}
	}

	if len(configs) <= 0 {
		return nil, dferrors.Newf(dfcodes.ManagerOrmStoreError, "config not exist, object %s, maxLen %d", object, maxLen)
	}

	sort.Sort(SortConfig(configs))
	len := uint32(len(configs))
	if maxLen == 0 || len <= maxLen {
		return configs, nil
	} else {
		return configs[0:maxLen], nil
	}
}
