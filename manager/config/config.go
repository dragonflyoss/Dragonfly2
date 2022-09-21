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
	"errors"
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/types"
)

type Config struct {
	// Base options.
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Server configuration.
	Server *ServerConfig `yaml:"server" mapstructure:"server"`

	// Database configuration.
	Database *DatabaseConfig `yaml:"database" mapstructure:"database"`

	// Cache configuration.
	Cache *CacheConfig `yaml:"cache" mapstructure:"cache"`

	// ObjectStorage configuration.
	ObjectStorage *ObjectStorageConfig `yaml:"objectStorage" mapstructure:"objectStorage"`

	// Metrics configuration.
	Metrics *MetricsConfig `yaml:"metrics" mapstructure:"metrics"`

	// Security configuration.
	Security *SecurityConfig `yaml:"security" mapstructure:"security"`
}

type ServerConfig struct {
	// Server name.
	Name string `yaml:"name" mapstructure:"name"`

	// Server dynamic config cache directory.
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`

	// Server log directory.
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// GRPC server configuration.
	GRPC *TCPListenConfig `yaml:"grpc" mapstructure:"grpc"`

	// REST server configuration.
	REST *RestConfig `yaml:"rest" mapstructure:"rest"`
}

type DatabaseConfig struct {
	// Database type.
	Type string `yaml:"type" mapstructure:"type"`

	// Mysql configuration.
	Mysql *MysqlConfig `yaml:"mysql" mapstructure:"mysql"`

	// Postgres configuration.
	Postgres *PostgresConfig `yaml:"postgres" mapstructure:"postgres"`

	// Redis configuration.
	Redis *RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type MysqlConfig struct {
	// Server username.
	User string `yaml:"user" mapstructure:"user"`

	// Server password.
	Password string `yaml:"password" mapstructure:"password"`

	// Server host.
	Host string `yaml:"host" mapstructure:"host"`

	// Server port.
	Port int `yaml:"port" mapstructure:"port"`

	// Server DB name.
	DBName string `yaml:"dbname" mapstructure:"dbname"`

	// TLS mode (can be one of "true", "false", "skip-verify",  or "preferred").
	TLSConfig string `yaml:"tlsConfig" mapstructure:"tlsConfig"`

	// Custom TLS configuration (overrides "TLSConfig" setting above).
	TLS *TLSConfig `yaml:"tls" mapstructure:"tls"`

	// Enable migration.
	Migrate bool `yaml:"migrate" mapstructure:"migrate"`
}

type TLSConfig struct {
	// Client certificate file path.
	Cert string `yaml:"cert" mapstructure:"cert"`

	// Client key file path.
	Key string `yaml:"key" mapstructure:"key"`

	// CA file path.
	CA string `yaml:"ca" mapstructure:"ca"`

	// InsecureSkipVerify controls whether a client verifies the
	// server's certificate chain and host name.
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" mapstructure:"insecureSkipVerify"`
}

type PostgresConfig struct {
	// Server username.
	User string `yaml:"user" mapstructure:"user"`

	// Server password.
	Password string `yaml:"password" mapstructure:"password"`

	// Server host.
	Host string `yaml:"host" mapstructure:"host"`

	// Server port.
	Port int `yaml:"port" mapstructure:"port"`

	// Server DB name.
	DBName string `yaml:"dbname" mapstructure:"dbname"`

	// SSL mode.
	SSLMode string `yaml:"sslMode" mapstructure:"sslMode"`

	// Server timezone.
	Timezone string `yaml:"timezone" mapstructure:"timezone"`

	// Enable migration.
	Migrate bool `yaml:"migrate" mapstructure:"migrate"`
}

type RedisConfig struct {
	// DEPRECATED: Please use the `addrs` field instead.
	Host string `yaml:"host" mapstructure:"host"`

	// DEPRECATED: Please use the `addrs` field instead.
	Port int `yaml:"port" mapstructure:"port"`

	// Server addresses.
	Addrs []string `yaml:"addrs" mapstructure:"addrs"`

	// Server username.
	Username string `yaml:"username" mapstructure:"username"`

	// Server password.
	Password string `yaml:"password" mapstructure:"password"`

	// Server cache DB name.
	DB int `yaml:"db" mapstructure:"db"`

	// Server broker DB name.
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// Server backend DB name.
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`
}

type CacheConfig struct {
	// Redis cache configuration.
	Redis *RedisCacheConfig `yaml:"redis" mapstructure:"redis"`

	// Local cache configuration.
	Local *LocalCacheConfig `yaml:"local" mapstructure:"local"`
}

type RedisCacheConfig struct {
	// Cache TTL.
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type LocalCacheConfig struct {
	// Size of LFU cache.
	Size int `yaml:"size" mapstructure:"size"`

	// Cache TTL.
	TTL time.Duration `yaml:"ttl" mapstructure:"ttl"`
}

type RestConfig struct {
	// REST server address.
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
	// DEPRECATED: Please use the `listenIP` field instead.
	Listen string `mapstructure:"listen" yaml:"listen"`

	// ListenIP is listen ip, like: 0.0.0.0, 192.168.0.1.
	ListenIP string `mapstructure:"listenIP" yaml:"listenIP"`

	// Port is listen port.
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

type SecurityConfig struct {
	// AutoIssueCert indicates to issue client certificates for all grpc call.
	AutoIssueCert bool `yaml:"autoIssueCert" mapstructure:"autoIssueCert"`

	// CACert is the CA certificate for all grpc tls handshake, it can be path or PEM format string.
	CACert types.PEMContent `mapstructure:"caCert" yaml:"caCert"`

	// CAKey is the CA private key, it can be path or PEM format string.
	CAKey types.PEMContent `mapstructure:"caKey" yaml:"caKey"`

	// TLSPolicy controls the grpc shandshake behaviors:
	// force: both ClientHandshake and ServerHandshake are only support tls
	// prefer: ServerHandshake supports tls and insecure (non-tls), ClientHandshake will only support tls
	// default: ServerHandshake supports tls and insecure (non-tls), ClientHandshake will only support insecure (non-tls)
	TLSPolicy string `mapstructure:"tlsPolicy" yaml:"tlsPolicy"`

	// CertSpec is the desired state of certificate.
	CertSpec *CertSpec `mapstructure:"certSpec" yaml:"certSpec"`
}

type CertSpec struct {
	// DNSNames is a list of dns names be set on the certificate.
	DNSNames []string `mapstructure:"dnsNames" yaml:"dnsNames"`

	// IPAddresses is a list of ip addresses be set on the certificate.
	IPAddresses []string `mapstructure:"ipAddresses" yaml:"ipAddresses"`

	// ValidityPeriod is the validity period  of certificate.
	ValidityPeriod time.Duration `mapstructure:"validityPeriod" yaml:"validityPeriod"`
}

// New config instance.
func New() *Config {
	return &Config{
		Server: &ServerConfig{
			Name: DefaultServerName,
			GRPC: &TCPListenConfig{
				ListenIP: DefaultGRPCListenIP,
				PortRange: TCPListenPortRange{
					Start: DefaultGRPCPort,
					End:   DefaultGRPCPort,
				},
			},
			REST: &RestConfig{
				Addr: DefaultRESTAddr,
			},
		},
		Database: &DatabaseConfig{
			Type: DatabaseTypeMysql,
			Mysql: &MysqlConfig{
				Port:    DefaultMysqlPort,
				DBName:  DefaultMysqlDBName,
				Migrate: true,
			},
			Postgres: &PostgresConfig{
				Port:     DefaultPostgresPort,
				DBName:   DefaultPostgresDBName,
				SSLMode:  DefaultPostgresSSLMode,
				Timezone: DefaultPostgresTimezone,
				Migrate:  true,
			},
			Redis: &RedisConfig{
				DB:        DefaultRedisDB,
				BrokerDB:  DefaultRedisBrokerDB,
				BackendDB: DefaultRedisBackendDB,
			},
		},
		Cache: &CacheConfig{
			Redis: &RedisCacheConfig{
				TTL: DefaultRedisCacheTTL,
			},
			Local: &LocalCacheConfig{
				Size: DefaultLFUCacheSize,
				TTL:  DefaultLFUCacheTTL,
			},
		},
		ObjectStorage: &ObjectStorageConfig{
			Enable: false,
		},
		Security: &SecurityConfig{
			AutoIssueCert: false,
			TLSPolicy:     rpc.PreferTLSPolicy,
			CertSpec: &CertSpec{
				IPAddresses:    DefaultCertIPAddresses,
				DNSNames:       DefaultCertDNSNames,
				ValidityPeriod: DefaultCertValidityPeriod,
			},
		},
		Metrics: &MetricsConfig{
			Enable:          false,
			Addr:            DefaultMetricsAddr,
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

	if cfg.Server.GRPC.ListenIP == "" {
		return errors.New("grpc requires parameter listenIP")
	}

	if cfg.Server.REST == nil {
		return errors.New("server requires parameter rest")
	}

	if cfg.Database == nil {
		return errors.New("config requires parameter database")
	}

	if cfg.Database.Type == "" {
		return errors.New("database requires parameter type")
	}

	if cfg.Database.Type == DatabaseTypeMysql || cfg.Database.Type == DatabaseTypeMariaDB {
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
	}

	if cfg.Database.Type == DatabaseTypePostgres {
		if cfg.Database.Postgres == nil {
			return errors.New("database requires parameter postgres")
		}

		if cfg.Database.Postgres.User == "" {
			return errors.New("postgres requires parameter user")
		}

		if cfg.Database.Postgres.Password == "" {
			return errors.New("postgres requires parameter password")
		}

		if cfg.Database.Postgres.Host == "" {
			return errors.New("postgres requires parameter host")
		}

		if cfg.Database.Postgres.Port <= 0 {
			return errors.New("postgres requires parameter port")
		}

		if cfg.Database.Postgres.DBName == "" {
			return errors.New("postgres requires parameter dbname")
		}

		if cfg.Database.Postgres.SSLMode == "" {
			return errors.New("postgres requires parameter sslMode")
		}

		if cfg.Database.Postgres.Timezone == "" {
			return errors.New("postgres requires parameter timezone")
		}
	}

	if cfg.Database.Redis == nil {
		return errors.New("database requires parameter redis")
	}

	if len(cfg.Database.Redis.Addrs) == 0 {
		return errors.New("redis requires parameter addrs")
	}

	if len(cfg.Database.Redis.Addrs) == 1 {
		if cfg.Database.Redis.DB < 0 {
			return errors.New("redis requires parameter db")
		}

		if cfg.Database.Redis.BrokerDB < 0 {
			return errors.New("redis requires parameter brokerDB")
		}

		if cfg.Database.Redis.BackendDB < 0 {
			return errors.New("redis requires parameter backendDB")
		}
	}

	if cfg.Database.Redis.BrokerDB < 0 {
		return errors.New("redis requires parameter brokerDB")
	}

	if cfg.Database.Redis.BackendDB < 0 {
		return errors.New("redis requires parameter backendDB")
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

		if !slices.Contains([]string{objectstorage.ServiceNameS3, objectstorage.ServiceNameOSS}, cfg.ObjectStorage.Name) {
			return errors.New("objectStorage requires parameter name")
		}

		if cfg.ObjectStorage.AccessKey == "" {
			return errors.New("objectStorage requires parameter accessKey")
		}

		if cfg.ObjectStorage.SecretKey == "" {
			return errors.New("objectStorage requires parameter secretKey")
		}
	}

	if cfg.Security != nil && cfg.Security.AutoIssueCert {
		if cfg.Security.CACert == "" {
			return errors.New("security requires parameter caCert")
		}

		if cfg.Security.CAKey == "" {
			return errors.New("security requires parameter caKey")
		}

		if !slices.Contains([]string{rpc.DefaultTLSPolicy, rpc.ForceTLSPolicy, rpc.PreferTLSPolicy}, cfg.Security.TLSPolicy) {
			return errors.New("security requires parameter tlsPolicy")
		}

		if cfg.Security.CertSpec == nil {
			return errors.New("security requires parameter certSpec")
		}

		if len(cfg.Security.CertSpec.IPAddresses) == 0 {
			return errors.New("certSpec requires parameter ipAddresses")
		}

		if len(cfg.Security.CertSpec.DNSNames) == 0 {
			return errors.New("certSpec requires parameter dnsNames")
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

func (cfg *Config) Convert() error {
	// TODO Compatible with deprecated fields host and port.
	if len(cfg.Database.Redis.Addrs) == 0 && cfg.Database.Redis.Host != "" && cfg.Database.Redis.Port > 0 {
		cfg.Database.Redis.Addrs = []string{fmt.Sprintf("%s:%d", cfg.Database.Redis.Host, cfg.Database.Redis.Port)}
	}

	// TODO Compatible with deprecated fields listen.
	if cfg.Server.GRPC.Listen != "" && cfg.Server.GRPC.ListenIP == "" {
		cfg.Server.GRPC.ListenIP = cfg.Server.GRPC.Listen
	}

	return nil
}
