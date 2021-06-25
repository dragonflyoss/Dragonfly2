package cache

import (
	"fmt"

	"d7y.io/dragonfly/v2/manager/config"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
)

type Cache struct {
	*cache.Cache
}

// New cache instance
func New(cfg *config.Config) *Cache {
	var localCache *cache.TinyLFU
	if cfg.Cache != nil {
		localCache = cache.NewTinyLFU(cfg.Cache.Size, cfg.Cache.TTL)
	}

	// If the attribute TTL of cache.Item(cache's instance) is 0, redis expiration time is 1 hour.
	// cfg.TTL Set the expiration time of TinyLFU.
	return &Cache{cache.New(&cache.Options{
		Redis:      newRedis(cfg.Database.Redis),
		LocalCache: localCache,
	})}
}

func newRedis(cfg *config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}

func MakeCacheKey(namespace string, id string) string {
	return fmt.Sprintf("dragonfly_manager:%s:%s", namespace, id)
}
