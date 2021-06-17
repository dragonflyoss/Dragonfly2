package config

import (
	"errors"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Server       *ServerConfig `yaml:"server" mapstructure:"server"`
	Redis        *RedisConfig  `yaml:"redis" mapstructure:"redis"`
	Mysql        *MysqlConfig  `yaml:"mysql" mapstructure:"mysql"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Addr     string `yaml:"addr" mapstructure:"addr"`
	Db       string `yaml:"db" mapstructure:"db"`
}

type RedisConfig struct {
	User     string   `yaml:"user" mapstructure:"user"`
	Password string   `yaml:"password" mapstructure:"password"`
	Addrs    []string `yaml:"addr" mapstructure:"addrs"`
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
