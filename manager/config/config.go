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
	"d7y.io/dragonfly/v2/pkg/objectstorage"
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

	// ObjectStorage configuration
	ObjectStorage *ObjectStorageConfig `yaml:"objectStorage" mapstructure:"objectStorage"`

	// Metrics configuration
	Metrics *MetricsConfig `yaml:"metrics" mapstructure:"metrics"`
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

	// TLS mode (can be one of "true", "false", "skip-verify",  or "preferred")
	TLSConfig string `yaml:"tlsConfig" mapstructure:"tlsConfig"`

	// Custom TLS configuration (overrides "TLSConfig" setting above)
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

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Enable peer gauge metrics.
	EnablePeerGauge bool `yaml:"enablePeerGauge" mapstructure:"enablePeerGauge"`
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

type ObjectStorageConfig struct {
	// Enable object storage.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Object storage name of type, it can be s3 or oss.
	Name string `mapstructure:"name" yaml:"name"`

	// Storage region.
	Region string `mapstructure:"region" yaml:"region"`

	// Datacenter endpoint.
	Endpoint string `mapstructure:"endpoint" yaml:"endpoint"`

	// Access key ID.
	AccessKey string `mapstructure:"accessKey" yaml:"accessKey"`

	// Access key secret.
	SecretKey string `mapstructure:"secretKey" yaml:"secretKey"`
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
				TTL:  10 * time.Second,
			},
		},
		ObjectStorage: &ObjectStorageConfig{
			Enable: false,
		},
		Metrics: &MetricsConfig{
			Enable:          false,
			EnablePeerGauge: true,
		},
	}
}

// Validate config values
func (cfg *Config) Validate() error {
	if cfg.Server == nil {
		return errors.New("config requires parameter server")
	}

	if cfg.Server.Name == "" {
		return errors.New("server requires parameter name")
	}

	if cfg.Server.GRPC == nil {
		return errors.New("server requires parameter grpc")
	}

	if cfg.Server.REST == nil {
		return errors.New("server requires parameter rest")
	}

	if cfg.Database == nil {
		return errors.New("config requires parameter database")
	}

	if cfg.Database.Redis == nil {
		return errors.New("database requires parameter redis")
	}

	if cfg.Database.Redis.Host == "" {
		return errors.New("redis requires parameter host")
	}

	if cfg.Database.Redis.Port <= 0 {
		return errors.New("redis requires parameter port")
	}

	if cfg.Database.Redis.CacheDB < 0 {
		return errors.New("redis requires parameter cacheDB")
	}

	if cfg.Database.Redis.BrokerDB < 0 {
		return errors.New("redis requires parameter brokerDB")
	}

	if cfg.Database.Redis.BackendDB < 0 {
		return errors.New("redis requires parameter backendDB")
	}

	if cfg.Database.Mysql == nil {
		return errors.New("database requires parameter mysql")
	}

	if cfg.Database.Mysql.User == "" {
		return errors.New("mysql requires parameter user")
	}

	if cfg.Database.Mysql.Password == "" {
		return errors.New("mysql requires parameter password")
	}

	if cfg.Database.Mysql.Host == "" {
		return errors.New("mysql requires parameter host")
	}

	if cfg.Database.Mysql.Port <= 0 {
		return errors.New("mysql requires parameter port")
	}

	if cfg.Database.Mysql.DBName == "" {
		return errors.New("mysql requires parameter dbname")
	}

	if cfg.Database.Mysql.TLS != nil {
		if cfg.Database.Mysql.TLS.Cert == "" {
			return errors.New("tls requires parameter cert")
		}

		if cfg.Database.Mysql.TLS.Key == "" {
			return errors.New("tls requires parameter key")
		}

		if cfg.Database.Mysql.TLS.CA == "" {
			return errors.New("tls requires parameter ca")
		}
	}

	if cfg.Cache == nil {
		return errors.New("config requires parameter cache")
	}

	if cfg.Cache.Redis == nil {
		return errors.New("cache requires parameter redis")
	}

	if cfg.Cache.Redis.TTL == 0 {
		return errors.New("redis requires parameter ttl")
	}

	if cfg.Cache.Local == nil {
		return errors.New("cache requires parameter local")
	}

	if cfg.Cache.Local.Size == 0 {
		return errors.New("local requires parameter size")
	}

	if cfg.Cache.Local.TTL == 0 {
		return errors.New("local requires parameter ttl")
	}

	if cfg.ObjectStorage != nil && cfg.ObjectStorage.Enable {
		if cfg.ObjectStorage.Name == "" {
			return errors.New("objectStorage requires parameter name")
		}

		if cfg.ObjectStorage.Name != objectstorage.ServiceNameS3 && cfg.ObjectStorage.Name != objectstorage.ServiceNameOSS {
			return errors.New("objectStorage requires parameter name")
		}

		if cfg.ObjectStorage.AccessKey == "" {
			return errors.New("objectStorage requires parameter accessKey")
		}

		if cfg.ObjectStorage.SecretKey == "" {
			return errors.New("objectStorage requires parameter secretKey")
		}
	}

	if cfg.Metrics == nil {
		return errors.New("config requires parameter metrics")
	}

	if cfg.Metrics.Enable {
		if cfg.Metrics.Addr == "" {
			return errors.New("metrics requires parameter addr")
		}
	}

	return nil
}
