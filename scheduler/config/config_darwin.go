// +build darwin

package config

import (
	"runtime"

	"d7y.io/dragonfly/v2/pkg/basic"
)

var (
	SchedulerConfigPath = basic.HomeDir + "/.dragonfly/scheduler.yaml"
)

var SchedulerConfig = Config{
	Console: false,
	Verbose: false,
	Server: serverConfig{
		Port: 8002,
	},
	Worker: schedulerWorkerConfig{
		WorkerNum:         runtime.GOMAXPROCS(0),
		WorkerJobPoolSize: 10000,
		SenderNum:         10,
		SenderJobPoolSize: 10000,
	},
	Scheduler: schedulerConfig{
		ABTest: false,
	},
	CDN: cdnConfig{
		Servers: []CdnServerConfig{
			{
				Name:         "cdn",
				IP:           "127.0.0.1",
				RpcPort:      8003,
				DownloadPort: 8001,
			},
		},
	},
	GC: gcConfig{
		TaskDelay:     3600 * 1000,
		PeerTaskDelay: 3600 * 1000,
	},
}
