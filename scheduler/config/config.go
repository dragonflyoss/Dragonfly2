package config

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"runtime"
)

var config = createDefaultConfig()

type Config struct {
	Scheduler schedulerConfig
	Server    serverConfig
	Worker    schedulerWorkerConfig
	CDN       cdnConfig
}

type schedulerConfig struct {
	MaxUsableValue float64
	MaxUploadPieceNum int32
	MaxDownloadPieceNum int32
}

type serverConfig struct {
	Type basic.NetworkType
	Addr string
}

type schedulerWorkerConfig struct {
	WorkerNum         int
	WorkerJobPoolSize int
	SenderNum         int
	SenderJobPoolSize int
}

type cdnConfig struct {
	List [][]serverConfig
}

func GetConfig() *Config {
	return config
}

func createDefaultConfig() *Config {
	return &Config{
		Server: serverConfig{
			Type: basic.TCP,
			Addr: ":6666",
		},
		Worker: schedulerWorkerConfig{
			WorkerNum:         runtime.GOMAXPROCS(0),
			WorkerJobPoolSize: 10000,
			SenderNum:         50,
			SenderJobPoolSize: 10000,
		},
		Scheduler: schedulerConfig{
			MaxUsableValue: 100,
			MaxUploadPieceNum: 5,
			MaxDownloadPieceNum: 5,
		},
		CDN: cdnConfig{
			List: [][]serverConfig{
				{{
					Type: basic.TCP,
					Addr: ":6666",
				}},
			},
		},
	}
}
