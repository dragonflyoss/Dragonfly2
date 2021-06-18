package config

import (
	"errors"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Server       *ServerConfig `yaml:"server" mapstructure:"server"`
	Redis        *RedisConfig  `yaml:"redis" mapstructure:"redis"`
	Mysql        *MysqlConfig  `yaml:"mysql" mapstructure:"mysql"`
	Cache        *CacheConfig  `yaml:"cache" mapstructure:"cache"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	DBName   string `yaml:"dbname" mapstructure:"dbname"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" mapstructure:"addr"`
	Password string `yaml:"password" mapstructure:"password"`
	DB       int    `yaml:"db" mapstructure:"db"`
}

type CacheConfig struct {
	Size int           `yaml:"size" mapstructure:"size"`
	TTL  time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

func New() *Config {
	return &config
}

func (cfg *Config) Validate() error {
	if cfg.Redis == nil {
		return errors.New("empty redis config is not specified")
	}

	if cfg.Mysql == nil {
		return errors.New("empty mysql config is not specified")
	}

	if cfg.Server == nil {
		return errors.New("empty server config is not specified")
	}

	return nil
}
