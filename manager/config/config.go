package config

const (
	DefaultConfigFilePath string = "/etc/dragonfly/manager.yml"
)

type Config struct {
	Server        *ServerConfig        `yaml:"server"`
	ConfigService *ConfigServiceConfig `yaml:"config-service"`
	Stores        []*StoreConfig       `yaml:"stores"`
}

type ServerConfig struct {
	IP   string `yaml:"ip",omitempty`
	Port int    `yaml:"port"`
}

type ConfigServiceConfig struct {
	StoreName string `yaml:"store-name"`
}

type MysqlConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	IP       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	Db       string `yaml:"db"`
	Table    string `yaml:"table"`
}

type OssConfig struct {
}

type MemoryConfig struct {
}

type StoreConfig struct {
	Name   string        `yaml:"name"`
	Type   string        `yaml:"type"`
	Mysql  *MysqlConfig  `yaml:"mysql",omitempty`
	Oss    *OssConfig    `yaml:"oss",omitempty`
	Memory *MemoryConfig `yaml:"memory", omitempty`
}

func GetConfig() *Config {
	return &Config{
		Server: &ServerConfig{
			Port: 8004,
		},
		ConfigService: &ConfigServiceConfig{
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
					Table:    "config_table",
				},
				Oss:    nil,
				Memory: nil,
			},
		},
	}
}

func (cfg *Config) CheckValid() error {
	return nil
}
