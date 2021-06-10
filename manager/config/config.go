package config

import (
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Server       *ServerConfig    `yaml:"server" mapstructure:"server"`
	Configure    *ConfigureConfig `yaml:"configure" mapstructure:"configure"`
	Redis        *RedisConfig     `yaml:"redis" mapstructure:"redis"`
	Stores       []*StoreConfig   `yaml:"stores" mapstructure:"stores"`
	HostService  *HostService     `yaml:"host-service" mapstructure:"host-service"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type ConfigureConfig struct {
	StoreName string `yaml:"store-name" mapstructure:"store-name"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Addr     string `yaml:"addr" mapstructure:"addr"`
	Db       string `yaml:"db" mapstructure:"db"`
}

type SQLiteConfig struct {
	Db string `yaml:"db" mapstructure:"db"`
}

type OssConfig struct {
}

// Only one of Mysql, SQLit and Oss can be used at the same time
type StoreSource struct {
	Mysql  *MysqlConfig  `yaml:"mysql,omitempty" mapstructure:"mysql,omitempty"`
	SQLite *SQLiteConfig `yaml:"sqlite,omitempty" mapstructure:"sqlite,omitempty"`
	Oss    *OssConfig    `yaml:"oss,omitempty" mapstructure:"oss,omitempty"`
}

type StoreConfig struct {
	Name   string       `yaml:"name" mapstructure:"name"`
	Source *StoreSource `yaml:"source" mapstructure:"source"`
}

type HostService struct {
}

type RedisConfig struct {
	User     string   `yaml:"user" mapstructure:"user"`
	Password string   `yaml:"password" mapstructure:"password"`
	Addrs    []string `yaml:"addr" mapstructure:"addrs"`
}

type SkylineService struct {
	Domain    string `yaml:"domain" mapstructure:"domain"`
	AppName   string `yaml:"app-name" mapstructure:"app-name"`
	Account   string `yaml:"account" mapstructure:"account"`
	AccessKey string `yaml:"access-key" mapstructure:"access-key"`
}

func New() *Config {
	return &Config{
		Server: &ServerConfig{
			Port: 8004,
		},
		Configure: &ConfigureConfig{
			StoreName: "store1",
		},
		Redis: &RedisConfig{
			User:     "",
			Password: "",
			Addrs:    []string{"127.0.0.1:6379"},
		},
		Stores: []*StoreConfig{
			{
				Name: "store1",
				Source: &StoreSource{
					Mysql: &MysqlConfig{
						User:     "root",
						Password: "root1234",
						Addr:     "127.0.0.1:3306",
						Db:       "dragonfly_manager",
					},
				},
			},
		},
		HostService: &HostService{},
	}
}

func (cfg *StoreConfig) Valid() (string, error) {
	if len(cfg.Name) <= 0 {
		return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Name is null")
	}

	if cfg.Source == nil {
		return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source is null")
	}

	source := cfg.Source
	if source.Mysql != nil {
		if len(source.Mysql.User) == 0 {
			return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.Mysql.User is null")
		}

		if len(source.Mysql.Password) == 0 {
			return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.Mysql.Password is null")
		}

		if len(source.Mysql.Addr) == 0 {
			return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.Mysql.Addr is null")
		}

		if len(source.Mysql.Db) == 0 {
			return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.Mysql.Db is null")
		}

		return "mysql", nil
	}

	if source.SQLite != nil {
		if len(source.SQLite.Db) == 0 {
			return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.SQLite.Db is null")
		}

		return "sqlite", nil
	}

	if source.Oss != nil {
		return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source.Oss not support yet")
	}

	return "", dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Source must be set one of mysql, sqlite, oss")
}

func (cfg *RedisConfig) Valid() error {
	if len(cfg.Addrs) == 0 {
		return dferrors.Newf(dfcodes.ManagerConfigError, "redis config error: Addrs is null")
	}

	return nil
}

func (cfg *Config) Valid() error {
	if cfg.Redis == nil {
		return dferrors.Newf(dfcodes.ManagerConfigError, "redis config error: Redis is null")
	}

	if err := cfg.Redis.Valid(); err != nil {
		return err
	}

	if len(cfg.Stores) <= 0 {
		return dferrors.Newf(dfcodes.ManagerConfigError, "stores config error: Stores is null")
	}

	for _, store := range cfg.Stores {
		if _, err := store.Valid(); err != nil {
			return err
		}
	}

	return nil
}
