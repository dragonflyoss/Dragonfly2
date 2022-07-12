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

	"github.com/go-redis/cache/v8"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
)

const (
	// Seed Peer prefix of cache key.
	SeedPeerNamespace = "seed-peer"

	// Peer prefix of cache key.
	PeerNamespace = "peer"

	// Scheduler prefix of cache key.
	SchedulerNamespace = "scheduler"

	// Schedulers prefix of cache key.
	SchedulersNamespace = "schedulers"

	// Buckets prefix of cache key.
	BucketsNamespace = "buckets"
)

const (
	// PeerCacheTTL is the ttl for peer cache.
	PeerCacheTTL = 30 * time.Minute
)

// Cache is cache client.
type Cache struct {
	*cache.Cache
	TTL time.Duration
}

// New cache instance.
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

// Make cache key.
func MakeCacheKey(namespace string, id string) string {
	return fmt.Sprintf("manager:%s:%s", namespace, id)
}

// Make cache key for seed peer.
func MakeSeedPeerCacheKey(hostname string, clusterID uint) string {
	return MakeCacheKey(SeedPeerNamespace, fmt.Sprintf("%s-%d", hostname, clusterID))
}

// Make cache key for peer.
func MakePeerCacheKey(hostname, ip string) string {
	return MakeCacheKey(PeerNamespace, fmt.Sprintf("%s-%s", hostname, ip))
}

// Make cache key for scheduler.
func MakeSchedulerCacheKey(hostname string, clusterID uint) string {
	return MakeCacheKey(SchedulerNamespace, fmt.Sprintf("%s-%d", hostname, clusterID))
}

// Make cache key for schedulers.
func MakeSchedulersCacheKey(hostname, ip string) string {
	return MakeCacheKey(SchedulersNamespace, fmt.Sprintf("%s-%s", hostname, ip))
}

// Make cache key for buckets.
func MakeBucketsCacheKey(name string) string {
	return MakeCacheKey(BucketsNamespace, name)
}

func MakeModelKey(clusterID uint, hostname, ip, id, version string) string {
	return fmt.Sprintf("%s:%s", MakeVersionKey(clusterID, hostname, ip, id), version)
}

func MakeVersionKey(clusterID uint, hostname, ip, id string) string {
	return fmt.Sprintf("%s:%s", MakePrefixKey(clusterID, hostname, ip), id)
}

func MakePrefixKey(clusterID uint, hostname, ip string) string {
	return fmt.Sprintf("manager:%s:%d:%s-%s", SchedulerNamespace, clusterID, hostname, ip)
}
