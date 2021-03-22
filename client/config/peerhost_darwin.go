// +build darwin

package config

import (
	"net"
	"time"

	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var (
	PeerHostConfigPath = basic.HomeDir + "/.dragonfly/dfget-daemon.yml"

	peerHostWorkHome = basic.HomeDir + "/.dragonfly/dfdaemon/"
	peerHostDataDir  = peerHostWorkHome
)

var PeerHostConfig = PeerHostOption{
	DataDir:     peerHostDataDir,
	WorkHome:    peerHostWorkHome,
	AliveTime:   clientutil.Duration{Duration: 5 * time.Minute},
	GCInterval:  clientutil.Duration{Duration: 1 * time.Minute},
	PidFile:     "/tmp/dfdaemon.pid",
	LockFile:    "/tmp/dfdaemon.lock",
	KeepStorage: false,
	Verbose:     false,
	Console:     false,
	Scheduler: SchedulerOption{
		NetAddrs:        nil,
		ScheduleTimeout: clientutil.Duration{Duration: 1 * time.Minute},
	},
	Host: HostOption{
		ListenIP:       net.IPv4zero.String(),
		AdvertiseIP:    iputils.HostIp,
		SecurityDomain: "",
		Location:       "",
		IDC:            "",
		NetTopology:    "",
	},
	Download: DownloadOption{
		RateLimit: clientutil.RateLimit{
			Limit: rate.Limit(104857600),
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
			Limit: rate.Limit(104857600),
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
		Option: storage.Option{
			TaskExpireTime: clientutil.Duration{
				Duration: 3 * time.Minute,
			},
		},
		StoreStrategy: storage.SimpleLocalTaskStoreStrategy,
	},
}
