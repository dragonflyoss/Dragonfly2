package config

import (
	"errors"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Server       *ServerConfig `yaml:"server" mapstructure:"server"`
	Database     *MysqlConfig  `yaml:"database" mapstructure:"database"`
	Cache        *CacheConfig  `yaml:"cache" mapstructure:"cache"`
}

type ServerConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	DBName   string `yaml:"dbname" mapstructure:"dbname"`
}

type CacheConfig struct {
	Redis      *RedisConfig      `yaml:"redis" mapstructure:"redis"`
	LocalCache *LocalCacheConfig `yaml:"localCache" mapstructure:"localCache"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" mapstructure:"addr"`
	Password string `yaml:"password" mapstructure:"password"`
	DB       int    `yaml:"db" mapstructure:"db"`
}

type LocalCacheConfig struct {
	Size int           `yaml:"size" mapstructure:"size"`
	TTL  time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

func New() *Config {
	return &config
}

func (cfg *Config) Validate() error {
	if cfg.Cache == nil {
		return errors.New("empty cache config is not specified")
	}

	if cfg.Cache != nil && cfg.Cache.Redis == nil {
		return errors.New("empty cache redis config is not specified")
	}

	if cfg.Database == nil {
		return errors.New("empty mysql config is not specified")
	}

	if cfg.Server == nil {
		return errors.New("empty server config is not specified")
	}

	return nil
}
