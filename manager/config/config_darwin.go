// +build darwin

package config

import "time"

var config = Config{
	Server: &ServerConfig{
		Port: 8002,
	},
	Cache: &CacheConfig{
		Size: 100000,
		TTL:  time.Minute,
	},
}
