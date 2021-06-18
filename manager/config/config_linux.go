// +build linux

package config

import "time"

var config = Config{
	Server: &ServerConfig{
		Addr: ":8080",
	},
	Cache: &CacheConfig{
		Size: 100000,
		TTL:  time.Minute,
	},
}
