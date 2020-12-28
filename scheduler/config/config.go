package config

import (
	"runtime"
)

const (
	DefaultConfigFilePath string = "conf/scheduler.yml"
)

var config = createDefaultConfig()

type Config struct {
	Scheduler schedulerConfig       `yaml:"scheduler"`
	Server    serverConfig          `yaml:"server"`
	Worker    schedulerWorkerConfig `yaml:"worker"`
	CDN       cdnConfig             `yaml:"cdn"`
	GC       gcConfig             `yaml:"gc"`
}

type schedulerConfig struct {
	MaxUsableValue      float64 `yaml:"max-usable-value"`
	MaxUploadPieceNum   int32   `yaml:"max-upload-piece-num"`
	MaxDownloadPieceNum int32   `yaml:"max-download-piece-num"`
}

type serverConfig struct {
	IP   string `yaml:"ip",omitempty`
	Port int    `yaml:"port"`
}

type schedulerWorkerConfig struct {
	WorkerNum         int `yaml:"worker-num"`
	WorkerJobPoolSize int `yaml:"worker-job-pool-size"`
	SenderNum         int `yaml:"sender-num"`
	SenderJobPoolSize int `yaml:"sender-job-pool-size"`
}

type cdnConfig struct {
	List [][]serverConfig `yaml:"list"`
}

type gcConfig struct {
	PeerTaskDelay int64 `yaml:"task-gc-delay-time"`
	TaskDelay int64 `yaml:"peer-task-gc-delay-time"`
}

func GetConfig() *Config {
	return config
}

func SetConfig(cfg *Config) {
	config = cfg
}

func createDefaultConfig() *Config {
	return &Config{
		Server: serverConfig{
			Port: 8002,
		},
		Worker: schedulerWorkerConfig{
			WorkerNum:         runtime.GOMAXPROCS(0),
			WorkerJobPoolSize: 10000,
			SenderNum:         50,
			SenderJobPoolSize: 10000,
		},
		Scheduler: schedulerConfig{
			MaxUsableValue:      100,
			MaxUploadPieceNum:   5,
			MaxDownloadPieceNum: 5,
		},
		CDN: cdnConfig{
			List: [][]serverConfig{
				{{
					IP:   "127.0.0.1",
					Port: 12345,
				}},
			},
		},
		GC: gcConfig{
			TaskDelay: 3600*1000,
			PeerTaskDelay: 24*3600*1000,
		},
	}
}
