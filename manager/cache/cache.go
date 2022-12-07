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
	SeedPeerNamespace = "seed-peers"

	// Peer prefix of cache key.
	PeerNamespace = "peers"

	// Scheduler prefix of cache key.
	SchedulerNamespace = "schedulers"

	// Applications prefix of cache key.
	ApplicationsNamespace = "applications"

	// Buckets prefix of cache key.
	BucketsNamespace = "buckets"
)

// Cache is cache client.
type Cache struct {
	*cache.Cache
	TTL time.Duration
}

// New cache instance.
func New(cfg *config.Config) (*Cache, error) {
	var localCache *cache.TinyLFU
	localCache = cache.NewTinyLFU(cfg.Cache.Local.Size, cfg.Cache.Local.TTL)

	rdb, err := database.NewRedis(&cfg.Database.Redis)
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

// Make namespace cache key.
func MakeNamespaceCacheKey(namespace string) string {
	return fmt.Sprintf("manager:%s", namespace)
}

// Make cache key.
func MakeCacheKey(namespace string, id string) string {
	return fmt.Sprintf("%s:%s", MakeNamespaceCacheKey(namespace), id)
}

// Make cache key for seed peer.
func MakeSeedPeerCacheKey(clusterID uint, hostname, ip string) string {
	return MakeCacheKey(SeedPeerNamespace, fmt.Sprintf("%d-%s-%s", clusterID, hostname, ip))
}

// Make cache key for scheduler.
func MakeSchedulerCacheKey(clusterID uint, hostname, ip string) string {
	return MakeCacheKey(SchedulerNamespace, fmt.Sprintf("%d-%s-%s", clusterID, hostname, ip))
}

// Make cache key for peer.
func MakePeerCacheKey(hostname, ip string) string {
	return MakeCacheKey(PeerNamespace, fmt.Sprintf("%s-%s", hostname, ip))
}

// Make schedulers cache key for peer.
func MakeSchedulersCacheKeyForPeer(hostname, ip string) string {
	return MakeCacheKey(PeerNamespace, fmt.Sprintf("%s-%s:schedulers", hostname, ip))
}

// Make applications cache key.
func MakeApplicationsCacheKey() string {
	return MakeNamespaceCacheKey(ApplicationsNamespace)
}

// Make cache key for bucket.
func MakeBucketCacheKey(name string) string {
	return MakeCacheKey(BucketsNamespace, name)
}

// Make cache model key.
func MakeModelKey(clusterID uint, hostname, ip, id string) string {
	return MakeCacheKey(SchedulerNamespace, fmt.Sprintf("%d-%s-%s:models:%s", clusterID, hostname, ip, id))
}

// Make cache model version key.
func MakeModelVersionKey(clusterID uint, hostname, ip, id, version string) string {
	return MakeCacheKey(SchedulerNamespace, fmt.Sprintf("%d-%s-%s:models:%s:versions:%s", clusterID, hostname, ip, id, version))
}
