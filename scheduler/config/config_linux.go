// +build linux

package config

import (
	"runtime"
)

var config = Config{
	Dynconfig: &DynconfigOptions{
		Type:       1,
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
