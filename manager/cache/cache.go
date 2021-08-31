/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import (
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"github.com/go-redis/cache/v8"
)

const (
	CDNNamespace        = "cdn"
	SchedulerNamespace  = "scheduler"
	SchedulersNamespace = "schedulers"
)

type Cache struct {
	*cache.Cache
	TTL time.Duration
}

// New cache instance
func New(cfg *config.Config) (*Cache, error) {
	var localCache *cache.TinyLFU
	if cfg.Cache != nil {
		localCache = cache.NewTinyLFU(cfg.Cache.Local.Size, cfg.Cache.Local.TTL)
	}

	rdb, err := database.NewRedis(cfg.Database.Redis)
	if err != nil {
		return nil, err
	}

	// If the attribute TTL of cache.Item(cache's instance) is 0, redis expiration time is 1 hour.
	// cfg.TTL Set the expiration time of TinyLFU.
	return &Cache{
		Cache: cache.New(&cache.Options{
			Redis:      rdb,
			LocalCache: localCache,
		}),
		TTL: cfg.Cache.Redis.TTL,
	}, nil
}

func MakeCacheKey(namespace string, id string) string {
	return fmt.Sprintf("manager:%s:%s", namespace, id)
}

func MakeCDNCacheKey(hostname string, clusterID uint) string {
	return MakeCacheKey(CDNNamespace, fmt.Sprintf("%s-%d", hostname, clusterID))
}

func MakeSchedulerCacheKey(hostname string, clusterID uint) string {
	return MakeCacheKey(SchedulerNamespace, fmt.Sprintf("%s-%d", hostname, clusterID))
}

func MakeSchedulersCacheKey(hostname string) string {
	return MakeCacheKey(SchedulersNamespace, hostname)
}
