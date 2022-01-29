/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/docker/go-connections/tlsconfig"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

type Config struct {
	// Base options
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Server configuration
	Server *ServerConfig `yaml:"server" mapstructure:"server"`

	// Database configuration
	Database *DatabaseConfig `yaml:"database" mapstructure:"database"`

	// Cache configuration
	Cache *CacheConfig `yaml:"cache" mapstructure:"cache"`

	// Metrics configuration
	Metrics *RestConfig `yaml:"metrics" mapstructure:"metrics"`
}

type ServerConfig struct {
	// Server name
	Name string `yaml:"name" mapstructure:"name"`

	// Server log directory
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Console resource path
	PublicPath string `yaml:"publicPath" mapstructure:"publicPath"`

	// GRPC server configuration
	GRPC *TCPListenConfig `yaml:"grpc" mapstructure:"grpc"`

	// REST server configuration
	REST *RestConfig `yaml:"rest" mapstructure:"rest"`
}

type DatabaseConfig struct {
	// Mysql configuration
	Mysql *MysqlConfig `yaml:"mysql" mapstructure:"mysql"`

	// Redis configuration
	Redis *RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type MysqlConfig struct {
	// Server username
	User string `yaml:"user" mapstructure:"user"`

	// Server password
	Password string `yaml:"password" mapstructure:"password"`

	// Server host
	Host string `yaml:"host" mapstructure:"host"`

	// Server port
	Port int `yaml:"port" mapstructure:"port"`

	// Server DB name
	DBName string `yaml:"dbname" mapstructure:"dbname"`

	// Enable migration
	Migrate bool `yaml:"migrate" mapstructure:"migrate"`

	// TLS configuration
	TLS *TLSConfig `yaml:"tls" mapstructure:"tls"`
}

type TLSConfig struct {
	// Client certificate file path
	Cert string `yaml:"cert" mapstructure:"cert"`

	// Client key file path
	Key string `yaml:"key" mapstructure:"key"`

	// CA file path
	CA string `yaml:"ca" mapstructure:"ca"`

	// InsecureSkipVerify controls whether a client verifies the
	// server's certificate chain and host name.
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" mapstructure:"insecureSkipVerify"`
}

// Generate client tls config
func (t *TLSConfig) Client() (*tls.Config, error) {
	return tlsconfig.Client(tlsconfig.Options{
		CAFile:             t.CA,
		CertFile:           t.Cert,
		KeyFile:            t.Key,
		InsecureSkipVerify: t.InsecureSkipVerify,
	})
}

type RedisConfig struct {
	// Server host
	Host string `yaml:"host" mapstructure:"host"`

	// Server port
	Port int `yaml:"port" mapstructure:"port"`

	// Server password
	Password string `yaml:"password" mapstructure:"password"`

	// Server cache DB name
	CacheDB int `yaml:"cacheDB" mapstructure:"cacheDB"`

	// Server broker DB name
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// Server backend DB name
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`
}

type CacheConfig struct {
	// Redis cache configuration
	Redis *RedisCacheConfig `yaml:"redis" mapstructure:"redis"`

	// Local cache configuration
	Local *LocalCacheConfig `yaml:"local" mapstructure:"local"`
}

type RedisCacheConfig struct {
	// Cache TTL
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type LocalCacheConfig struct {
	// Size of LFU cache
	Size int `yaml:"size" mapstructure:"size"`

	// Cache TTL
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type RestConfig struct {
	// REST server address
	Addr string `yaml:"addr" mapstructure:"addr"`
}

type TCPListenConfig struct {
	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `mapstructure:"listen" yaml:"listen"`

	// PortRange stands listen port
	PortRange TCPListenPortRange `yaml:"port" mapstructure:"port"`
}

type TCPListenPortRange struct {
	Start int
	End   int
}

// New config instance
func New() *Config {
	return &Config{
		Server: &ServerConfig{
			Name:       "d7y/manager",
			PublicPath: "manager/console/dist",
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
		Database: &DatabaseConfig{
			Redis: &RedisConfig{
				CacheDB:   0,
				BrokerDB:  1,
				BackendDB: 2,
			},
			Mysql: &MysqlConfig{
				Migrate: true,
			},
		},
		Cache: &CacheConfig{
			Redis: &RedisCacheConfig{
				TTL: 30 * time.Second,
			},
			Local: &LocalCacheConfig{
				Size: 10000,
				TTL:  30 * time.Second,
			},
		},
	}
}

// Validate config values
func (cfg *Config) Validate() error {
	if cfg.Server == nil {
		return errors.New("empty server config is not specified")
	}

	if cfg.Server.Name == "" {
		return errors.New("empty server name config is not specified")
	}

	if cfg.Server.GRPC == nil {
		return errors.New("empty grpc server config is not specified")
	}

	if cfg.Server.REST == nil {
		return errors.New("empty rest server config is not specified")
	}

	if cfg.Database == nil {
		return errors.New("empty database config is not specified")
	}

	if cfg.Database.Redis == nil {
		return errors.New("empty database redis config is not specified")
	}

	if cfg.Database.Redis.Host == "" {
		return errors.New("empty database redis host is not specified")
	}

	if cfg.Database.Redis.Port <= 0 {
		return errors.New("empty database redis port is not specified")
	}

	if cfg.Database.Redis.CacheDB < 0 {
		return errors.New("empty database redis cacheDB is not specified")
	}

	if cfg.Database.Redis.BrokerDB < 0 {
		return errors.New("empty database redis brokerDB is not specified")
	}

	if cfg.Database.Redis.BackendDB < 0 {
		return errors.New("empty database redis backendDB is not specified")
	}

	if cfg.Database.Mysql == nil {
		return errors.New("empty database mysql config is not specified")
	}

	if cfg.Database.Mysql.User == "" {
		return errors.New("empty database mysql user is not specified")
	}

	if cfg.Database.Mysql.Password == "" {
		return errors.New("empty database mysql password is not specified")
	}

	if cfg.Database.Mysql.Host == "" {
		return errors.New("empty database mysql host is not specified")
	}

	if cfg.Database.Mysql.Port <= 0 {
		return errors.New("empty database mysql port is not specified")
	}

	if cfg.Database.Mysql.DBName == "" {
		return errors.New("empty database mysql dbName is not specified")
	}

	if cfg.Database.Mysql.TLS != nil {
		if cfg.Database.Mysql.TLS.Cert == "" {
			return errors.New("empty database mysql tls cert is not specified")
		}

		if cfg.Database.Mysql.TLS.Key == "" {
			return errors.New("empty database mysql tls key is not specified")
		}

		if cfg.Database.Mysql.TLS.CA == "" {
			return errors.New("empty database mysql tls ca is not specified")
		}
	}

	if cfg.Cache == nil {
		return errors.New("empty cache config is not specified")
	}

	if cfg.Cache.Redis == nil {
		return errors.New("empty cache redis config is not specified")
	}

	if cfg.Cache.Redis.TTL == 0 {
		return errors.New("empty cache redis TTL is not specified")
	}

	if cfg.Cache.Local == nil {
		return errors.New("empty cache local config is not specified")
	}

	if cfg.Cache.Local.Size == 0 {
		return errors.New("empty cache local size is not specified")
	}

	if cfg.Cache.Local.TTL == 0 {
		return errors.New("empty cache local TTL is not specified")
	}

	if cfg.Metrics != nil && cfg.Metrics.Addr == "" {
		return errors.New("empty metrics addr is not specified")
	}

	return nil
}
