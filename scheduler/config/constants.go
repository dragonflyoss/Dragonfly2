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
	"time"
)

const (
	// DefaultSeedPeerConcurrentUploadLimit is default number for seed peer concurrent upload limit.
	DefaultSeedPeerConcurrentUploadLimit = 300

	// DefaultPeerConcurrentUploadLimit is default number for peer concurrent upload limit.
	DefaultPeerConcurrentUploadLimit = 50

	// DefaultPeerParallelCount is default number for pieces to download in parallel.
	DefaultPeerParallelCount = 4

	// DefaultSchedulerFilterParentLimit is default limit the number for filter traversals.
	DefaultSchedulerFilterParentLimit = 4

	// DefaultSchedulerFilterParentRangeLimit is default limit the range for filter traversals.
	DefaultSchedulerFilterParentRangeLimit = 40
)

const (
	// DefaultServerPort is default port for server.
	DefaultServerPort = 8002
)

const (
	// DefaultSchedulerAlgorithm is default algorithm for scheduler.
	DefaultSchedulerAlgorithm = "default"

	// DefaultSchedulerBackSourceCount is default back-to-source count for scheduler.
	DefaultSchedulerBackSourceCount = 3

	// DefaultSchedulerRetryBackSourceLimit is default retry back-to-source limit for scheduler.
	DefaultSchedulerRetryBackSourceLimit = 5

	// DefaultSchedulerRetryLimit is default retry limit for scheduler.
	DefaultSchedulerRetryLimit = 10

	// DefaultSchedulerRetryInterval is default retry interval for scheduler.
	DefaultSchedulerRetryInterval = 50 * time.Millisecond

	// DefaultSchedulerPeerGCInterval is default interval for peer gc.
	DefaultSchedulerPeerGCInterval = 10 * time.Second

	// DefaultSchedulerPeerTTL is default ttl for peer.
	DefaultSchedulerPeerTTL = 24 * time.Hour

	// DefaultSchedulerTaskGCInterval is default interval for task gc.
	DefaultSchedulerTaskGCInterval = 30 * time.Minute

	// DefaultSchedulerHostGCInterval is default interval for host gc.
	DefaultSchedulerHostGCInterval = 1 * time.Hour

	// DefaultRefreshModelInterval is model refresh interval.
	DefaultRefreshModelInterval = 168 * time.Hour

	// DefaultCPU is default cpu usage.
	DefaultCPU = 1
)

const (
	// DefaultDynConfigRefreshInterval is default refresh interval for dynamic configuration.
	DefaultDynConfigRefreshInterval = 10 * time.Second
)

const (
	// DefaultManagerSchedulerClusterID is default id for scheduler cluster.
	DefaultManagerSchedulerClusterID = 1

	// DefaultManagerKeepAliveInterval is default interval for keepalive.
	DefaultManagerKeepAliveInterval = 5 * time.Second
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
