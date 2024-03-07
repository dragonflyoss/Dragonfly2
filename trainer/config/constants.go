/*
 *     Copyright 2023 The Dragonfly Authors
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
	// DefaultServerPort is default port for server.
	DefaultServerPort = 9090

	// DefaultServerAdvertisePort is default advertise port for server.
	DefaultServerAdvertisePort = 9090
)

const (
	// DefaultMetricsAddr is default address for metrics server.
	DefaultMetricsAddr = ":8000"
)

var (
	// DefaultCertIPAddresses is default ip addresses of certificate.
	DefaultCertIPAddresses = []net.IP{ip.IPv4, ip.IPv6}

	// DefaultCertDNSNames is default dns names of certificate.
	DefaultCertDNSNames = []string{"dragonfly-trainer", "dragonfly-trainer.dragonfly-system.svc", "dragonfly-trainer.dragonfly-system.svc.cluster.local"}

	// DefaultCertValidityPeriod is default validity period of certificate.
	DefaultCertValidityPeriod = 180 * 24 * time.Hour
)

var (
	// DefaultNetworkEnableIPv6 is default value of enableIPv6.
	DefaultNetworkEnableIPv6 = false
)

const (
	// DefaultLogRotateMaxSize is the default maximum size in megabytes of log files before rotation.
	DefaultLogRotateMaxSize = 1024

	// DefaultLogRotateMaxAge is the default number of days to retain old log files.
	DefaultLogRotateMaxAge = 7

	// DefaultLogRotateMaxBackups is the default number of old log files to keep.
	DefaultLogRotateMaxBackups = 20
)
