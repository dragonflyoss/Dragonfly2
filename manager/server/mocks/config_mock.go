package mocks

import (
	"path"

	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/manager/config"
)

func NewMockConfig() *config.Config {
	return &config.Config{
		Server: &config.ServerConfig{
			Port: 8004,
		},
		Configure: &config.ConfigureConfig{
			StoreName: "store1",
		},
		Redis: &config.RedisConfig{
			User:     "",
			Password: "",
			Addrs:    []string{"127.0.0.1:6379"},
		},
		Stores: []*config.StoreConfig{
			{
				Name: "store1",
				Source: &config.StoreSource{
					SQLite: &config.SQLiteConfig{
						Db: path.Join(dfpath.WorkHome, "dragonfly_manager.db"),
					},
				},
			},
		},
		HostService: &config.HostService{},
	}
}
