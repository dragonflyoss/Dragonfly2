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

	"github.com/dragonflyoss/Dragonfly2/client/config"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"golang.org/x/time/rate"
)

var flagDaemonOpt = config.PeerHostOption{
	DataDir:     "",
	WorkHome:    "",
	AliveTime:   config.Duration{Duration: 5 * time.Minute},
	GCInterval:  config.Duration{Duration: 1 * time.Minute},
	Schedulers:  nil,
	PidFile:     "/tmp/dfdaemon.pid",
	LockFile:    "/tmp/dfdaemon.lock",
	KeepStorage: false,
	Verbose:     false,
	Host: config.HostOption{
		AdvertiseIP:    net.IPv4zero,
		SecurityDomain: "",
		Location:       "",
		IDC:            "",
		NetTopology:    "",
	},
	Download: config.DownloadOption{
		RateLimit: config.RateLimit{
			Limit: rate.Limit(104857600),
		},
		DownloadGRPC: config.ListenOption{
			UnixListen: &config.UnixListenOption{
				Socket: "/tmp/dfdamon.sock",
			},
		},
		PeerGRPC: config.ListenOption{
			TCPListen: &config.TCPListenOption{
				PortRange: config.TCPListenPortRange{
					Start: 65000,
					End:   65000,
				},
			},
		},
	},
	Upload: config.UploadOption{
		RateLimit: config.RateLimit{
			Limit: rate.Limit(104857600),
		},
		ListenOption: config.ListenOption{
			TCPListen: &config.TCPListenOption{
				PortRange: config.TCPListenPortRange{
					Start: 65002,
					End:   65002,
				},
			},
		},
	},
	Proxy: &config.ProxyOption{
		ListenOption: config.ListenOption{
			TCPListen: &config.TCPListenOption{
				PortRange: config.TCPListenPortRange{
					Start: 65001,
					End:   65001,
				},
			},
		},
	},
	Storage: config.StorageOption{
		Option: storage.Option{
			TaskExpireTime: 3 * time.Minute,
		},
		StoreStrategy: storage.StoreStrategy(string(storage.SimpleLocalTaskStoreStrategy)),
	},
}
