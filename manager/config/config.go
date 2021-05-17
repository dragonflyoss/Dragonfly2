package config

import (
	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	base.Options  `yaml:",inline" mapstructure:",squash"`
	Server        *ServerConfig        `yaml:"server" mapstructure:"server"`
	ConfigService *ConfigServiceConfig `yaml:"config-service" mapstructure:"config-service"`
	Stores        []*StoreConfig       `yaml:"stores" mapstructure:"stores"`
}

type ServerConfig struct {
	IP   string `yaml:"ip,omitempty" mapstructure:"ip,omitempty"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type ConfigServiceConfig struct {
	StoreName string `yaml:"store-name"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	IP       string `yaml:"ip" mapstructure:"ip"`
	Port     int    `yaml:"port" mapstructure:"port"`
	Db       string `yaml:"db" mapstructure:"db"`
	Table    string `yaml:"table" mapstructure:"table"`
}

type OssConfig struct {
}

type MemoryConfig struct {
}

type StoreConfig struct {
	Name   string        `yaml:"name" mapstructure:"name"`
	Type   string        `yaml:"type" mapstructure:"type"`
	Mysql  *MysqlConfig  `yaml:"mysql,omitempty" mapstructure:"mysql,omitempty"`
	Oss    *OssConfig    `yaml:"oss,omitempty" mapstructure:"oss,omitempty"`
	Memory *MemoryConfig `yaml:"memory,omitempty" mapstructure:"memory,omitempty"`
}

func New() *Config {
	return &Config{
		Server: &ServerConfig{
			Port: 8004,
		},
		ConfigService: &ConfigServiceConfig{
			StoreName: "store1",
		},
		Stores: []*StoreConfig{
			{
				Name: "store1",
				Type: "mysql",
				Mysql: &MysqlConfig{
					User:     "root",
					Password: "root1234",
					IP:       "127.0.0.1",
					Port:     3306,
					Db:       "config_db",
					Table:    "config_table",
				},
				Oss:    nil,
				Memory: nil,
			},
		},
	}
}

func (cfg *Config) CheckValid() error {
	return nil
}
