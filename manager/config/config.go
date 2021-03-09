package config

const (
	DefaultConfigFilePath string = "conf/manager.yml"
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
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	IP       string `yaml:"ip"`
	Port     int    `yaml:"port"`
	DbName   string `yaml:"dbname"`
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
			StoreName: "config_db",
		},
		Stores: []*StoreConfig{
			{
				Name: "config_db",
				Type: "mysql",
				Mysql: &MysqlConfig{
					Username: "root",
					Password: "root1234",
					IP:       "127.0.0.1",
					Port:     3306,
					DbName:   "config_db",
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
