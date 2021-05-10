package config

const (
	DefaultConfigFilePath string = "/etc/dragonfly/manager.yaml"
)

type Config struct {
	Console     bool             `yaml:"console"`
	Verbose     bool             `yaml:"verbose"`
	PProfPort   int              `yaml:"pprofPort"`
	Server      *ServerConfig    `yaml:"server"`
	Configure   *ConfigureConfig `yaml:"configure"`
	Stores      []*StoreConfig   `yaml:"stores"`
	HostService *HostService     `yaml:"host-service"`
}

type ServerConfig struct {
	IP   string `yaml:"ip"`
	Port int    `yaml:"port"`
}

type ConfigureConfig struct {
	StoreName string `yaml:"store-name"`
}

type MysqlConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	IP       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	Db       string `yaml:"db"`
}

type OssConfig struct {
}

type StoreConfig struct {
	Name  string       `yaml:"name"`
	Type  string       `yaml:"type"`
	Mysql *MysqlConfig `yaml:"mysql,omitempty"`
	Oss   *OssConfig   `yaml:"oss,omitempty"`
}

type HostService struct {
	Skyline *SkylineService `yaml:"skyline"`
}

type SkylineService struct {
	Domain    string `yaml:"domain"`
	AppName   string `yaml:"app-name"`
	Account   string `yaml:"account"`
	AccessKey string `yaml:"access-key"`
}

func New() *Config {
	return &Config{
		Server: &ServerConfig{
			Port: 8004,
		},
		Configure: &ConfigureConfig{
			StoreName: "store1",
		},
		Stores: []*StoreConfig{
			{
				Name: "store1",
				Type: "mysql",
				Mysql: &MysqlConfig{
					User:     "root",
					Password: "root1234",
					IP:       "127.0.0.1",
					Port:     3306,
					Db:       "config_db",
				},
				Oss: nil,
			},
		},
		HostService: &HostService{
			Skyline: &SkylineService{
				Domain:    "http://xxx",
				AppName:   "dragonfly-manager",
				Account:   "yourAccount",
				AccessKey: "yourAccessKey",
			},
		},
	}
}

func (cfg *Config) CheckValid() error {
	return nil
}
