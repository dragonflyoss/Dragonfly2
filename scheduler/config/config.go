package config

var config = createDefaultConfig()

type Config struct {
	Scheduler schedulerConfig
	Server serverConfig
	Worker schedulerWorkerConfig
}

type schedulerConfig struct {
	MaxUsableValue float64
}

type serverConfig struct {
	Addr string
	Port int
}

type schedulerWorkerConfig struct {
	WorkerNum int
	SenderNum int
}

func GetConfig() *Config {
	return config
}

func createDefaultConfig() *Config {
	return &Config {
		Server: serverConfig{
			Port: 6666,
		},
		Scheduler: schedulerConfig{
			MaxUsableValue: 100,
		},
		Worker: schedulerWorkerConfig{
			WorkerNum: 4,
			SenderNum: 50,
		},
	}
}