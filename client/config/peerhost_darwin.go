// +build darwin

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

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var (
	PeerHostConfigPath = basic.HomeDir + "/.dragonfly/dfget-daemon.yaml"

	peerHostWorkHome = basic.HomeDir + "/.dragonfly/dfdaemon/"
	peerHostDataDir  = peerHostWorkHome
)

var peerHostConfig = DaemonOption{
	DataDir:     peerHostDataDir,
	WorkHome:    peerHostWorkHome,
	AliveTime:   clientutil.Duration{Duration: DefaultDaemonAliveTime},
	GCInterval:  clientutil.Duration{Duration: DefaultGCInterval},
	KeepStorage: false,
	Scheduler: SchedulerOption{
		NetAddrs: []dfnet.NetAddr{
			{
				Type: dfnet.TCP,
				Addr: "127.0.0.1:8002",
			},
		},
		ScheduleTimeout: clientutil.Duration{Duration: DefaultScheduleTimeout},
	},
	Host: HostOption{
		ListenIP:       net.IPv4zero.String(),
		AdvertiseIP:    iputils.HostIP,
		SecurityDomain: "",
		Location:       "",
		IDC:            "",
		NetTopology:    "",
	},
	Download: DownloadOption{
		TotalRateLimit: clientutil.RateLimit{
			Limit: rate.Limit(DefaultTotalDownloadLimit),
		},
		PerPeerRateLimit: clientutil.RateLimit{
			Limit: rate.Limit(DefaultPerPeerDownloadLimit),
		},
		DownloadGRPC: ListenOption{
			Security: SecurityOption{
				Insecure: true,
			},
			UnixListen: &UnixListenOption{
				Socket: "/tmp/dfdaemon.sock",
			},
		},
		PeerGRPC: ListenOption{
			Security: SecurityOption{
				Insecure: true,
			},
			TCPListen: &TCPListenOption{
				Listen: net.IPv4zero.String(),
				PortRange: TCPListenPortRange{
					Start: 65000,
					End:   65535,
				},
			},
		},
	},
	Upload: UploadOption{
		RateLimit: clientutil.RateLimit{
			Limit: rate.Limit(DefaultUploadLimit),
		},
		ListenOption: ListenOption{
			Security: SecurityOption{
				Insecure: true,
			},
			TCPListen: &TCPListenOption{
				Listen: net.IPv4zero.String(),
				PortRange: TCPListenPortRange{
					Start: 65002,
					End:   65535,
				},
			},
		},
	},
	Proxy: &ProxyOption{
		ListenOption: ListenOption{
			Security: SecurityOption{
				Insecure: true,
			},
			TCPListen: &TCPListenOption{
				Listen:    net.IPv4zero.String(),
				PortRange: TCPListenPortRange{},
			},
		},
	},
	Storage: StorageOption{
		TaskExpireTime: clientutil.Duration{
			Duration: DefaultTaskExpireTime,
		},
		StoreStrategy: AdvanceLocalTaskStoreStrategy,
		Multiplex:     false,
	},
}
