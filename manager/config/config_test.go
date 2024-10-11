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
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/pkg/objectstorage"
)

var (
	mockJWTConfig = JWTConfig{
		Realm:      "foo",
		Key:        "bar",
		Timeout:    30 * time.Second,
		MaxRefresh: 1 * time.Minute,
	}

	mockMysqlConfig = MysqlConfig{
		User:      "foo",
		Password:  "bar",
		Host:      "localhost",
		Port:      DefaultMysqlPort,
		DBName:    DefaultMysqlDBName,
		TLSConfig: "true",
		Migrate:   true,
	}

	mockMysqlTLSConfig = &MysqlTLSClientConfig{
		Cert:               "ca.crt",
		Key:                "ca.key",
		CACert:             "ca",
		InsecureSkipVerify: false,
	}

	mockPostgresConfig = PostgresConfig{
		User:                 "foo",
		Password:             "bar",
		Host:                 "localhost",
		Port:                 DefaultPostgresPort,
		DBName:               DefaultPostgresDBName,
		SSLMode:              DefaultPostgresSSLMode,
		PreferSimpleProtocol: DefaultPostgresPreferSimpleProtocol,
		Timezone:             DefaultPostgresTimezone,
		Migrate:              true,
	}

	mockRedisConfig = RedisConfig{
		Addrs:      []string{"127.0.0.0:6379"},
		MasterName: "master",
		Username:   "baz",
		Password:   "bax",
		DB:         DefaultRedisDB,
		BrokerDB:   DefaultRedisBrokerDB,
		BackendDB:  DefaultRedisBackendDB,
	}

	mockObjectStorageConfig = ObjectStorageConfig{
		Enable:           true,
		Name:             objectstorage.ServiceNameS3,
		Region:           "bas",
		Endpoint:         "127.0.0.1",
		AccessKey:        "ak",
		SecretKey:        "sk",
		S3ForcePathStyle: true,
	}

	mockMetricsConfig = MetricsConfig{
		Enable: true,
		Addr:   DefaultMetricsAddr,
	}
)

func TestConfig_Load(t *testing.T) {
	config := &Config{
		Server: ServerConfig{
			Name:          "foo",
			CacheDir:      "foo",
			LogDir:        "foo",
			LogMaxSize:    512,
			LogMaxAge:     5,
			LogMaxBackups: 3,
			PluginDir:     "foo",
			GRPC: GRPCConfig{
				AdvertiseIP: net.IPv4zero,
				ListenIP:    net.IPv4zero,
				Port: TCPListenPortRange{
					Start: 65003,
					End:   65003,
				},
				TLS: &GRPCTLSServerConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "foo",
				},
			},
			REST: RESTConfig{
				Addr: ":8080",
				TLS: &RESTTLSServerConfig{
					Cert: "foo",
					Key:  "foo",
				},
			},
		},
		Auth: AuthConfig{
			JWT: JWTConfig{
				Realm:      "foo",
				Key:        "bar",
				Timeout:    30 * time.Second,
				MaxRefresh: 1 * time.Minute,
			},
		},
		Database: DatabaseConfig{
			Type: "mysql",
			Mysql: MysqlConfig{
				User:      "foo",
				Password:  "foo",
				Host:      "foo",
				Port:      3306,
				DBName:    "foo",
				TLSConfig: "preferred",
				TLS: &MysqlTLSClientConfig{
					CACert:             "foo",
					Cert:               "foo",
					Key:                "foo",
					InsecureSkipVerify: true,
				},
				Migrate: true,
			},
			Postgres: PostgresConfig{
				User:                 "foo",
				Password:             "foo",
				Host:                 "foo",
				Port:                 5432,
				DBName:               "foo",
				SSLMode:              "disable",
				PreferSimpleProtocol: false,
				Timezone:             "UTC",
				Migrate:              true,
			},
			Redis: RedisConfig{
				Password:   "bar",
				Addrs:      []string{"foo", "bar"},
				MasterName: "baz",
				DB:         0,
				BrokerDB:   1,
				BackendDB:  2,
			},
		},
		Cache: CacheConfig{
			Redis: RedisCacheConfig{
				TTL: 1 * time.Second,
			},
			Local: LocalCacheConfig{
				Size: 10000,
				TTL:  1 * time.Second,
			},
		},
		Job: JobConfig{
			RateLimit: RateLimitConfig{
				FillInterval: 1 * time.Second,
				Capacity:     1000,
				Quantum:      1000,
			},
			GC: GCConfig{
				Interval: 1 * time.Second,
				TTL:      1 * time.Second,
			},
			Preheat: PreheatConfig{
				RegistryTimeout: DefaultJobPreheatRegistryTimeout,
				TLS: PreheatTLSClientConfig{
					CACert: "foo",
				},
			},
			SyncPeers: SyncPeersConfig{
				Interval: 13 * time.Hour,
				Timeout:  2 * time.Minute,
			},
		},
		ObjectStorage: ObjectStorageConfig{
			Enable:           true,
			Name:             objectstorage.ServiceNameS3,
			Endpoint:         "127.0.0.1",
			AccessKey:        "foo",
			SecretKey:        "bar",
			Region:           "baz",
			S3ForcePathStyle: false,
		},
		Metrics: MetricsConfig{
			Enable: true,
			Addr:   ":8000",
		},
		Network: NetworkConfig{
			EnableIPv6: true,
		},
	}

	managerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/manager.yaml")
	if err := yaml.Unmarshal(contentYAML, &managerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert := assert.New(t)
	assert.EqualValues(config, managerConfigYAML)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		mock   func(cfg *Config)
		expect func(t *testing.T, err error)
	}{
		{
			name:   "valid config",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:   "server requires parameter name",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.Name = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter name")
			},
		},
		{
			name:   "grpc requires parameter advertiseIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.GRPC.AdvertiseIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "grpc requires parameter advertiseIP")
			},
		},
		{
			name:   "grpc requires parameter listenIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.GRPC.ListenIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "grpc requires parameter listenIP")
			},
		},
		{
			name:   "grpc tls requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.GRPC.TLS = &GRPCTLSServerConfig{
					CACert: "",
					Cert:   "foo",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "grpc tls requires parameter caCert")
			},
		},
		{
			name:   "grpc tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.GRPC.TLS = &GRPCTLSServerConfig{
					CACert: "foo",
					Cert:   "",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "grpc tls requires parameter cert")
			},
		},
		{
			name:   "grpc tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.GRPC.TLS = &GRPCTLSServerConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "grpc tls requires parameter key")
			},
		},
		{
			name:   "rest tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.REST.TLS = &RESTTLSServerConfig{
					Cert: "",
					Key:  "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rest tls requires parameter cert")
			},
		},
		{
			name:   "rest tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.REST.TLS = &RESTTLSServerConfig{
					Cert: "foo",
					Key:  "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rest tls requires parameter key")
			},
		},
		{
			name:   "jwt requires parameter realm",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT.Realm = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "jwt requires parameter realm")
			},
		},
		{
			name:   "jwt requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT.Key = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "jwt requires parameter key")
			},
		},
		{
			name:   "jwt requires parameter timeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Auth.JWT.Timeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "jwt requires parameter timeout")
			},
		},
		{
			name:   "jwt requires parameter maxRefresh",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Auth.JWT.MaxRefresh = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "jwt requires parameter maxRefresh")
			},
		},
		{
			name:   "database requires parameter type",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "database requires parameter type")
			},
		},
		{
			name:   "mysql requires parameter user",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.User = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql requires parameter user")
			},
		},
		{
			name:   "mysql requires parameter password",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.Password = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql requires parameter password")
			},
		},
		{
			name:   "mysql requires parameter host",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.Host = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql requires parameter host")
			},
		},
		{
			name:   "mysql requires parameter port",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.Port = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql requires parameter port")
			},
		},
		{
			name:   "mysql requires parameter dbname",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.DBName = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql requires parameter dbname")
			},
		},
		{
			name:   "mysql tls requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.TLS = mockMysqlTLSConfig
				cfg.Database.Mysql.TLS = &MysqlTLSClientConfig{
					CACert: "",
					Cert:   "foo",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql tls requires parameter caCert")
			},
		},
		{
			name:   "mysql tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.TLS = mockMysqlTLSConfig
				cfg.Database.Mysql.TLS = &MysqlTLSClientConfig{
					CACert: "foo",
					Cert:   "",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql tls requires parameter cert")
			},
		},
		{
			name:   "mysql tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Mysql.TLS = mockMysqlTLSConfig
				cfg.Database.Mysql.TLS = &MysqlTLSClientConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "mysql tls requires parameter key")
			},
		},
		{
			name:   "postgres requires parameter user",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.User = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter user")
			},
		},
		{
			name:   "postgres requires parameter password",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.Password = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter password")
			},
		},
		{
			name:   "postgres requires parameter host",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.Host = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter host")
			},
		},
		{
			name:   "postgres requires parameter port",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.Port = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter port")
			},
		},
		{
			name:   "postgres requires parameter dbname",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.DBName = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter dbname")
			},
		},
		{
			name:   "postgres requires parameter sslMode",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.SSLMode = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter sslMode")
			},
		},
		{
			name:   "postgres requires parameter timezone",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypePostgres
				cfg.Database.Postgres = mockPostgresConfig
				cfg.Database.Postgres.Timezone = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "postgres requires parameter timezone")
			},
		},
		{
			name:   "redis requires parameter addrs",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.Addrs = []string{}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter addrs")
			},
		},
		{
			name:   "redis requires parameter db",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.DB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter db")
			},
		},
		{
			name:   "redis requires parameter brokerDB",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.BrokerDB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter brokerDB")
			},
		},
		{
			name:   "redis requires parameter backendDB",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.BackendDB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter backendDB")
			},
		},
		{
			name:   "redis requires parameter ttl",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Cache.Redis.TTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter ttl")
			},
		},
		{
			name:   "local requires parameter size",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Cache.Local.Size = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "local requires parameter size")
			},
		},
		{
			name:   "local requires parameter ttl",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Cache.Local.TTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "local requires parameter ttl")
			},
		},
		{
			name:   "rateLimit requires parameter fillInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.RateLimit.FillInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rateLimit requires parameter fillInterval")
			},
		},
		{
			name:   "rateLimit requires parameter capacity",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.RateLimit.Capacity = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rateLimit requires parameter capacity")
			},
		},
		{
			name:   "rateLimit requires parameter quantum",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.RateLimit.Quantum = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "rateLimit requires parameter quantum")
			},
		},
		{
			name:   "gc requires parameter interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.GC.Interval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "gc requires parameter interval")
			},
		},
		{
			name:   "gc requires parameter ttl",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.GC.TTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "gc requires parameter ttl")
			},
		},
		{
			name:   "preheat requires parameter registryTimeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.Preheat.RegistryTimeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "preheat requires parameter registryTimeout")
			},
		},
		{
			name:   "syncPeers requires parameter interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.SyncPeers.Interval = 11 * time.Hour
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "syncPeers requires parameter interval and it must be greater than 12 hours")
			},
		},
		{
			name:   "syncPeers requires parameter timeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job.SyncPeers.Timeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "syncPeers requires parameter timeout")
			},
		},
		{
			name:   "objectStorage requires parameter name",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.ObjectStorage = mockObjectStorageConfig
				cfg.ObjectStorage.Name = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "objectStorage requires parameter name")
			},
		},
		{
			name:   "invalid objectStorage name",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.ObjectStorage = mockObjectStorageConfig
				cfg.ObjectStorage.Name = "foo"
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "objectStorage requires parameter name")
			},
		},
		{
			name:   "objectStorage requires parameter accessKey",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.ObjectStorage = mockObjectStorageConfig
				cfg.ObjectStorage.AccessKey = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "objectStorage requires parameter accessKey")
			},
		},
		{
			name:   "objectStorage requires parameter secretKey",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.ObjectStorage = mockObjectStorageConfig
				cfg.ObjectStorage.SecretKey = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "objectStorage requires parameter secretKey")
			},
		},
		{
			name:   "metrics requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Auth.JWT = mockJWTConfig
				cfg.Database.Type = DatabaseTypeMysql
				cfg.Database.Mysql = mockMysqlConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Metrics = mockMetricsConfig
				cfg.Metrics.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "metrics requires parameter addr")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Convert(); err != nil {
				t.Fatal(err)
			}

			tc.mock(tc.config)
			tc.expect(t, tc.config.Validate())
		})
	}
}
