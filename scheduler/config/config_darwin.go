// +build darwin

package config

import (
	"runtime"

	"d7y.io/dragonfly/v2/pkg/basic"
)

var (
	SchedulerConfigPath = basic.HomeDir + "/.dragonfly/scheduler.yaml"
)

var config = Config{
	Console: false,
	Verbose: true,
	Dynconfig: &DynconfigOptions{
		ExpireTime: 60000 * 1000 * 1000,
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
