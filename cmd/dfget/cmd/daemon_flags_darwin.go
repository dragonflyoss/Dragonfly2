/*
 * Copyright The Dragonfly Authors.
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

// +build darwin

package cmd

import (
	"net"
	"time"

	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/basic"
)

var (
	peerHostConfigPath = basic.HomeDir + "/.dragonfly/dfget-daemon.yml"
)

var flagDaemonOpt = config.PeerHostOption{
	DataDir:     "",
	WorkHome:    "",
	AliveTime:   clientutil.Duration{Duration: 5 * time.Minute},
	GCInterval:  clientutil.Duration{Duration: 1 * time.Minute},
	Schedulers:  nil,
	PidFile:     "/tmp/dfdaemon.pid",
	LockFile:    "/tmp/dfdaemon.lock",
	KeepStorage: false,
	Verbose:     false,
	Host: config.HostOption{
		ListenIP:       net.IPv4zero.String(),
		AdvertiseIP:    iputils.HostIp,
		SecurityDomain: "",
		Location:       "",
		IDC:            "",
		NetTopology:    "",
	},
	Download: config.DownloadOption{
		RateLimit: clientutil.RateLimit{
			Limit: rate.Limit(104857600),
		},
		DownloadGRPC: config.ListenOption{
			Security: config.SecurityOption{
				Insecure: true,
			},
			UnixListen: &config.UnixListenOption{
				Socket: "/tmp/dfdamon.sock",
			},
		},
		PeerGRPC: config.ListenOption{
			Security: config.SecurityOption{
				Insecure: true,
			},
			TCPListen: &config.TCPListenOption{
				Listen: net.IPv4zero.String(),
				PortRange: config.TCPListenPortRange{
					Start: 65000,
					End:   65535,
				},
			},
		},
	},
	Upload: config.UploadOption{
		RateLimit: clientutil.RateLimit{
			Limit: rate.Limit(104857600),
		},
		ListenOption: config.ListenOption{
			Security: config.SecurityOption{
				Insecure: true,
			},
			TCPListen: &config.TCPListenOption{
				Listen: net.IPv4zero.String(),
				PortRange: config.TCPListenPortRange{
					Start: 65002,
					End:   65535,
				},
			},
		},
	},
	Proxy: &config.ProxyOption{
		ListenOption: config.ListenOption{
			Security: config.SecurityOption{
				Insecure: true,
			},
			TCPListen: &config.TCPListenOption{
				Listen:    net.IPv4zero.String(),
				PortRange: config.TCPListenPortRange{},
			},
		},
	},
	Storage: config.StorageOption{
		Option: storage.Option{
			TaskExpireTime: clientutil.Duration{
				Duration: 3 * time.Minute,
			},
		},
		StoreStrategy: storage.SimpleLocalTaskStoreStrategy,
	},
}
