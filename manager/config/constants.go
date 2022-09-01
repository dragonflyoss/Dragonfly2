/*
 *     Copyright 2022 The Dragonfly Authors
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
	"time"

	"d7y.io/dragonfly/v2/pkg/net/ip"
)

const (
	// DatabaseTypeMysql is database type of mysql.
	DatabaseTypeMysql = "mysql"

	// DatabaseTypeMariaDB is database type of mariadb.
	DatabaseTypeMariaDB = "mariadb"

	// DatabaseTypePostgres is database type of postgres.
	DatabaseTypePostgres = "postgres"
)

const (
	// DefaultServerName is default server name.
	DefaultServerName = "d7y/manager"

	// DefaultGRPCPort is default port for grpc server.
	DefaultGRPCPort = 65003

	// DefaultRESTAddr is default address for rest server.
	DefaultRESTAddr = ":8080"
)

const (
	// DefaultRedisDB is default db for redis.
	DefaultRedisDB = 0

	// DefaultRedisBrokerDB is default db for redis broker.
	DefaultRedisBrokerDB = 1

	// DefaultRedisBackendDB is default db for redis backend.
	DefaultRedisBackendDB = 2
)

const (
	// DefaultRedisCacheTTL is default ttl for redis cache.
	DefaultRedisCacheTTL = 30 * time.Second

	// DefaultLFUCacheTTL is default ttl for lfu cache.
	DefaultLFUCacheTTL = 10 * time.Second

	// DefaultLFUCacheSize is default size for lfu cache.
	DefaultLFUCacheSize = 10000
)

const (
	// DefaultMysqlPort is default port for mysql.
	DefaultMysqlPort = 3306

	// DefaultMysqlDBName is default db name for mysql.
	DefaultMysqlDBName = "manager"
)

const (
	// DefaultPostgresPort is default port for postgres.
	DefaultPostgresPort = 5432

	// DefaultPostgresDBName is default db name for postgres.
	DefaultPostgresDBName = "manager"

	// DefaultPostgresSSLMode is default ssl mode for postgres.
	DefaultPostgresSSLMode = "disable"

	// DefaultPostgresTimezone is default timezone for postgres.
	DefaultPostgresTimezone = "UTC"
)

const (
	// DefaultMetricsAddr is default address for metrics server.
	DefaultMetricsAddr = ":8000"
)

var (
	// DefaultCertIPAddresses is default ip addresses of certificate.
	DefaultCertIPAddresses = []string{ip.IPv4}

	// DefaultCertDNSNames is default dns names of certificate.
	DefaultCertDNSNames = []string{"dragonfly-manager", "dragonfly-manager.dragonfly-system.svc", "dragonfly-manager.dragonfly-system.svc.cluster.local"}

	// DefaultCertValidityPeriod is default validity period of certificate.
	DefaultCertValidityPeriod = 10 * 365 * 24 * time.Hour
)
