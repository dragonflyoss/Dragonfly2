// +build linux

package config

import (
	"runtime"
	"time"

	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var config = Config{
	Dynconfig: &DynconfigOptions{
		Type:       dc.LocalSourceType,
		ExpireTime: 30000 * 1000 * 1000,
		Path:       SchedulerDynconfigPath,
		CachePath:  SchedulerDynconfigCachePath,
	},
	Server: ServerConfig{
		IP:   iputils.HostIP,
		Port: 8002,
	},
	Worker: SchedulerWorkerConfig{
		WorkerNum:         runtime.GOMAXPROCS(0),
		WorkerJobPoolSize: 10000,
		SenderNum:         10,
		SenderJobPoolSize: 10000,
	},
	Scheduler: SchedulerConfig{
		ABTest: false,
	},
	GC: GCConfig{
		TaskDelay:     3600 * 1000,
		PeerTaskDelay: 3600 * 1000,
	},
	Manager: ManagerConfig{
		KeepAlive: KeepAliveConfig{
			Interval:         5 * time.Second,
			RetryMaxAttempts: 100000000,
			RetryInitBackOff: 5,
			RetryMaxBackOff:  10,
		},
	},
}
