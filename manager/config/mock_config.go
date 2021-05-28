package config

func MockConfig() *Config {
	return &Config{
		Server: &ServerConfig{
			Port: 8004,
		},
		Configure: &ConfigureConfig{
			StoreName: "store1",
		},
		Redis: &RedisConfig{
			User:     "",
			Password: "",
			Addrs:    []string{"127.0.0.1:6379"},
		},
		Stores: []*StoreConfig{
			{
				Name: "store1",
				Type: "mysql",
				Mysql: &MysqlConfig{
					User:     "root",
					Password: "root1234",
					Addr:     "127.0.0.1:3306",
					Db:       "dragonfly_manager",
				},
				Oss: nil,
			},
		},
		HostService: &HostService{},
	}
}
