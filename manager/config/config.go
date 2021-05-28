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

type OssConfig struct {
}

type StoreConfig struct {
	Name  string       `yaml:"name" mapstructure:"name"`
	Type  string       `yaml:"type" mapstructure:"type"`
	Mysql *MysqlConfig `yaml:"mysql,omitempty" mapstructure:"mysql,omitempty"`
	Oss   *OssConfig   `yaml:"oss,omitempty" mapstructure:"oss,omitempty"`
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
				Type: "mysql",
				Mysql: &MysqlConfig{
					User:     "root",
					Password: "root1234",
					Addr:     "127.0.0.1:3306",
					Db:       "dragonfly_manager",
				},
				Oss: nil,
			},
		},
		HostService: &HostService{},
	}
}

func (cfg *StoreConfig) Valid() error {
	if (cfg.Mysql == nil && cfg.Oss == nil) || (cfg.Mysql != nil && cfg.Oss != nil) {
		return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: please select one of mysql or oss")
	}

	if cfg.Mysql != nil {
		if len(cfg.Mysql.User) == 0 {
			return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Mysql.User is null")
		}

		if len(cfg.Mysql.Password) == 0 {
			return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Mysql.Password is null")
		}

		if len(cfg.Mysql.Addr) == 0 {
			return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Mysql.Addr is null")
		}

		if len(cfg.Mysql.Db) == 0 {
			return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: Mysql.Db is null")
		}

		return nil
	}

	if cfg.Oss != nil {
		return dferrors.Newf(dfcodes.ManagerConfigError, "store config error: oss not support yet")
	}

	return nil
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

	return nil
}
