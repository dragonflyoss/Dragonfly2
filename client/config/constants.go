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
	"d7y.io/dragonfly/v2/pkg/unit"
)

// Download limit.
const (
	DefaultPerPeerDownloadLimit = 512 * unit.MB
	DefaultTotalDownloadLimit   = 1024 * unit.MB
	DefaultUploadLimit          = 1024 * unit.MB
	DefaultMinRate              = 20 * unit.MB
)

// Others.
const (
	DefaultTaskExpireTime  = 6 * time.Hour
	DefaultGCInterval      = 1 * time.Minute
	DefaultDaemonAliveTime = 5 * time.Minute
	DefaultScheduleTimeout = 5 * time.Minute

	DefaultSchedulerIP   = "127.0.0.1"
	DefaultSchedulerPort = 8002

	DefaultPieceDispatcherRandomRatio = 0.1
	DefaultObjectMaxReplicas          = 3
)

// Store strategy.
const (
	SimpleLocalTaskStoreStrategy  = StoreStrategy("io.d7y.storage.v2.simple")
	AdvanceLocalTaskStoreStrategy = StoreStrategy("io.d7y.storage.v2.advance")
)

// Dfcache subcommand names.
const (
	CmdStat   = "stat"
	CmdImport = "import"
	CmdExport = "export"
	CmdDelete = "delete"
)

// Service default port of listening.
const (
	DefaultEndPort                = 65535
	DefaultPeerStartPort          = 65000
	DefaultUploadStartPort        = 65002
	DefaultObjectStorageStartPort = 65004
	DefaultHealthyStartPort       = 40901
)

var (
	// DefaultCertIPAddresses is default ip addresses of certificate.
	DefaultCertIPAddresses = []net.IP{ip.IPv4, ip.IPv6}

	// DefaultCertDNSNames is default dns names of certificate.
	DefaultCertDNSNames = []string{"dragonfly-peer", "dragonfly-peer.dragonfly-system.svc", "dragonfly-peer.dragonfly-system.svc.cluster.local",
		"dragonfly-seed-peer", "dragonfly-seed-peer.dragonfly-system.svc", "dragonfly-seed-peer.dragonfly-system.svc.cluster.local",
		"dragonfly-proxy", "dragonfly-proxy.dragonfly-system.svc", "dragonfly-proxy.dragonfly-system.svc.cluster.local",
		"dragonfly-dfdaemon", "dragonfly-dfdaemon.dragonfly-system.svc", "dragonfly-dfdaemon.dragonfly-system.svc.cluster.local",
	}

	// DefaultCertValidityPeriod is default validity period of certificate.
	DefaultCertValidityPeriod = 180 * 24 * time.Hour
)

var (
	// DefaultAnnouncerSchedulerInterval is default interface of announcing scheduler.
	DefaultAnnouncerSchedulerInterval = 30 * time.Second
)

const (
	// DefaultProbeInterval is the default interval of probing host.
	DefaultProbeInterval = 20 * time.Minute
)

const (
	// DefaultLogRotateMaxSize is the default maximum size in megabytes of log files before rotation.
	DefaultLogRotateMaxSize = 1024

	// DefaultLogRotateMaxAge is the default number of days to retain old log files.
	DefaultLogRotateMaxAge = 7

	// DefaultLogRotateMaxBackups is the default number of old log files to keep.
	DefaultLogRotateMaxBackups = 20
)
