// +build darwin

package config

import (
	"runtime"

	dc "d7y.io/dragonfly/v2/internal/dynconfig"
)

var config = Config{
	Dynconfig: &DynconfigOptions{
		Type:       dc.LocalSourceType,
		ExpireTime: 60000 * 1000 * 1000,
		Path:       SchedulerDynconfigPath,
		CachePath:  SchedulerDynconfigCachePath,
	},
	Server: ServerConfig{
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
}
