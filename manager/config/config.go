package config

import (
	"errors"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Server       *ServerConfig   `yaml:"server" mapstructure:"server"`
	Database     *DatabaseConfig `yaml:"database" mapstructure:"database"`
	Cache        *CacheConfig    `yaml:"cache" mapstructure:"cache"`
}

type ServerConfig struct {
	GRPC *TCPListenConfig `yaml:"grpc" mapstructure:"grpc"`
	REST *RestConfig      `yaml:"rest" mapstructure:"rest"`
}

type DatabaseConfig struct {
	Mysql *MysqlConfig `yaml:"mysql" mapstructure:"mysql"`
	Redis *RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type MysqlConfig struct {
	User     string `yaml:"user" mapstructure:"user"`
	Password string `yaml:"password" mapstructure:"password"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	DBName   string `yaml:"dbname" mapstructure:"dbname"`
}

type RedisConfig struct {
	Host     string `yaml:"host" mapstructure:"host"`
	Port     string `yaml:"port" mapstructure:"port"`
	Password string `yaml:"password" mapstructure:"password"`
	DB       int    `yaml:"db" mapstructure:"db"`
}

type CacheConfig struct {
	Redis *RedisCacheConfig `yaml:"redis" mapstructure:"redis"`
	Local *LocalCacheConfig `yaml:"local" mapstructure:"local"`
}

type RedisCacheConfig struct {
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type LocalCacheConfig struct {
	Size int           `yaml:"size" mapstructure:"size"`
	TTL  time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type RestConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"`
}

type TCPListenConfig struct {
	// PortRange stands listen port
	PortRange TCPListenPortRange `yaml:"port" mapstructure:"port"`
}

type TCPListenPortRange struct {
	Start int
	End   int
}

func New() *Config {
	return &Config{
		Server: &ServerConfig{
			GRPC: &TCPListenConfig{
				PortRange: TCPListenPortRange{
					Start: 65003,
					End:   65003,
				},
			},
			REST: &RestConfig{
				Addr: ":8080",
			},
		},
		Cache: &CacheConfig{
			Redis: &RedisCacheConfig{
				TTL: 1 * time.Minute,
			},
			Local: &LocalCacheConfig{
				Size: 10000,
				TTL:  1 * time.Minute,
			},
		},
	}
}

func (cfg *Config) Validate() error {
	if cfg.Cache == nil {
		return errors.New("empty cache config is not specified")
	}

	if cfg.Cache != nil {
		if cfg.Cache.Redis.TTL == 0 {
			return errors.New("empty redis cache TTL is not specified")
		}

		if cfg.Cache.Local.Size == 0 {
			return errors.New("empty local cache size is not specified")
		}

		if cfg.Cache.Local.TTL == 0 {
			return errors.New("empty local cache TTL is not specified")
		}
	}

	if cfg.Database == nil {
		return errors.New("empty mysql config is not specified")
	}

	if cfg.Database != nil {
		if cfg.Database.Redis == nil {
			return errors.New("empty cache redis config is not specified")
		}

		if cfg.Database.Mysql == nil {
			return errors.New("empty cache mysql config is not specified")
		}
	}

	if cfg.Server == nil {
		return errors.New("empty server config is not specified")
	}

	if cfg.Server != nil {
		if cfg.Server.GRPC == nil {
			return errors.New("empty grpc config is not specified")
		}

		if cfg.Server.REST == nil {
			return errors.New("empty rest config is not specified")
		}
	}

	return nil
}
