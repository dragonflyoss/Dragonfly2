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
	"time"

	"d7y.io/dragonfly/v2/pkg/net/ip"
)

const (
	// DefaultSeedPeerConcurrentUploadLimit is default number for seed peer concurrent upload limit.
	DefaultSeedPeerConcurrentUploadLimit = 2000

	// DefaultPeerConcurrentUploadLimit is default number for peer concurrent upload limit.
	DefaultPeerConcurrentUploadLimit = 200

	// DefaultSchedulerCandidateParentLimit is default limit the number of candidate parent.
	DefaultSchedulerCandidateParentLimit = 4

	// DefaultSchedulerFilterParentLimit is default limit the number for filter parent.
	DefaultSchedulerFilterParentLimit = 15
)

const (
	// DefaultServerPort is default port for server.
	DefaultServerPort = 8002

	// DefaultServerAdvertisePort is default advertise port for server.
	DefaultServerAdvertisePort = 8002
)

const (
	// DefaultRedisBrokerDB is default db for redis broker.
	DefaultRedisBrokerDB = 1

	// DefaultRedisBackendDB is default db for redis backend.
	DefaultRedisBackendDB = 2
)

const (
	// DefaultSchedulerAlgorithm is default algorithm for scheduler.
	DefaultSchedulerAlgorithm = "default"

	// DefaultSchedulerBackToSourceCount is default back-to-source count for scheduler.
	DefaultSchedulerBackToSourceCount = 200

	// DefaultSchedulerRetryBackToSourceLimit is default retry back-to-source limit for scheduler.
	DefaultSchedulerRetryBackToSourceLimit = 4

	// DefaultSchedulerRetryLimit is default retry limit for scheduler.
	DefaultSchedulerRetryLimit = 5

	// DefaultSchedulerRetryInterval is default retry interval for scheduler.
	DefaultSchedulerRetryInterval = 500 * time.Millisecond

	// DefaultSchedulerPieceDownloadTimeout is default timeout of downloading piece.
	DefaultSchedulerPieceDownloadTimeout = 30 * time.Minute

	// DefaultSchedulerPeerGCInterval is default interval for peer gc.
	DefaultSchedulerPeerGCInterval = 10 * time.Second

	// DefaultSchedulerPeerTTL is default ttl for peer.
	DefaultSchedulerPeerTTL = 24 * time.Hour

	// DefaultSchedulerTaskGCInterval is default interval for task gc.
	DefaultSchedulerTaskGCInterval = 30 * time.Minute

	// DefaultSchedulerHostGCInterval is default interval for host gc.
	DefaultSchedulerHostGCInterval = 5 * time.Minute

	// DefaultSchedulerHostTTL is default ttl for host.
	DefaultSchedulerHostTTL = 1 * time.Hour

	// DefaultRefreshModelInterval is model refresh interval.
	DefaultRefreshModelInterval = 168 * time.Hour

	// DefaultCPU is default cpu usage.
	DefaultCPU = 1
)

const (
	// DefaultResourceTaskDownloadTinyScheme is default scheme of downloading tiny task.
	DefaultResourceTaskDownloadTinyScheme = "http"

	// DefaultResourceTaskDownloadTinyTimeout is default timeout of downloading tiny task.
	DefaultResourceTaskDownloadTinyTimeout = 1 * time.Minute
)

const (
	// DefaultDynConfigRefreshInterval is default refresh interval for dynamic configuration.
	DefaultDynConfigRefreshInterval = 1 * time.Minute
)

const (
	// DefaultManagerSchedulerClusterID is default id for scheduler cluster.
	DefaultManagerSchedulerClusterID = 1

	// DefaultManagerKeepAliveInterval is default interval for keepalive.
	DefaultManagerKeepAliveInterval = 5 * time.Second
)

const (
	// DefaultSeedTaskDownloadTimeout is default timeout of downloading task by seed peer.
	DefaultSeedPeerTaskDownloadTimeout = 10 * time.Hour
)

const (
	// DefaultJobGlobalWorkerNum is default global worker number for job.
	DefaultJobGlobalWorkerNum = 500

	// DefaultJobSchedulerWorkerNum is default scheduler worker number for job.
	DefaultJobSchedulerWorkerNum = 500

	// DefaultJobGlobalWorkerNum is default local worker number for job.
	DefaultJobLocalWorkerNum = 1000

	// DefaultJobRedisBrokerDB is default db for redis broker.
	DefaultJobRedisBrokerDB = 1

	// DefaultJobRedisBackendDB is default db for redis backend.
	DefaultJobRedisBackendDB = 2
)

const (
	// DefaultMetricsAddr is default address for metrics server.
	DefaultMetricsAddr = ":8000"
)

var (
	// DefaultCertIPAddresses is default ip addresses of certificate.
	DefaultCertIPAddresses = []net.IP{ip.IPv4, ip.IPv6}

	// DefaultCertDNSNames is default dns names of certificate.
	DefaultCertDNSNames = []string{"dragonfly-scheduler", "dragonfly-scheduler.dragonfly-system.svc", "dragonfly-scheduler.dragonfly-system.svc.cluster.local"}

	// DefaultCertValidityPeriod is default validity period of certificate.
	DefaultCertValidityPeriod = 180 * 24 * time.Hour
)

var (
	// DefaultNetworkEnableIPv6 is default value of enableIPv6.
	DefaultNetworkEnableIPv6 = false
)

const (
	// DefaultStorageMaxSize is the default maximum size of record file.
	DefaultStorageMaxSize = 100

	// DefaultStorageMaxBackups is the default maximum count of backup.
	DefaultStorageMaxBackups = 10

	// DefaultStorageBufferSize is the default size of buffer container.
	DefaultStorageBufferSize = 100
)

const (
	// DefaultLogRotateMaxSize is the default maximum size in megabytes of log files before rotation.
	DefaultLogRotateMaxSize = 1024

	// DefaultLogRotateMaxAge is the default number of days to retain old log files.
	DefaultLogRotateMaxAge = 7

	// DefaultLogRotateMaxBackups is the default number of old log files to keep.
	DefaultLogRotateMaxBackups = 20
)
