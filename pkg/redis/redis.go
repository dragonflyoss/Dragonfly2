/*
 *     Copyright 2023 The Dragonfly Authors
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

package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"d7y.io/dragonfly/v2/pkg/types"
)

const (
	// KeySeparator is the separator of redis key.
	KeySeparator = ":"
)

const (
	// SeedPeerNamespace prefix of seed peers namespace cache key.
	SeedPeersNamespace = "seed-peers"

	// PeersNamespace prefix of peers namespace cache key.
	PeersNamespace = "peers"

	// SchedulersNamespace prefix of schedulers namespace cache key.
	SchedulersNamespace = "schedulers"

	// SchedulerClustersNamespace prefix of scheduler clusters namespace cache key.
	SchedulerClustersNamespace = "scheduler-clusters"

	// TasksNamespace prefix of tasks namespace cache key.
	PersistentCacheTasksNamespace = "persistent-cache-tasks"

	// PersistentCachePeersNamespace prefix of persistent cache peers namespace cache key.
	PersistentCachePeersNamespace = "persistent-cache-peers"

	// PersistentPeersNamespace prefix of persistent peers namespace cache key.
	PersistentPeersNamespace = "persistent-peers"

	// PersistentCacheHostsNamespace prefix of persistent cache hosts namespace cache key.
	PersistentCacheHostsNamespace = "persistent-cache-hosts"

	// ApplicationsNamespace prefix of applications namespace cache key.
	ApplicationsNamespace = "applications"

	// RateLimitersNamespace prefix of rate limiters namespace cache key.
	RateLimitersNamespace = "rate-limiters"
)

// NewRedis returns a new redis client.
func NewRedis(cfg *redis.UniversalOptions) (redis.UniversalClient, error) {
	redis.SetLogger(&redisLogger{})
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:            cfg.Addrs,
		MasterName:       cfg.MasterName,
		DB:               cfg.DB,
		Username:         cfg.Username,
		Password:         cfg.Password,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: cfg.SentinelPassword,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return client, nil
}

// IsEnabled check redis is enabled.
func IsEnabled(addrs []string) bool {
	return len(addrs) != 0
}

// MakeNamespaceKeyInManager make namespace key in manager.
func MakeNamespaceKeyInManager(namespace string) string {
	return fmt.Sprintf("%s:%s", types.ManagerName, namespace)
}

// MakeKeyInManager make key in manager.
func MakeKeyInManager(namespace, id string) string {
	return fmt.Sprintf("%s:%s", MakeNamespaceKeyInManager(namespace), id)
}

// MakeSeedPeerKeyInManager make seed peer key in manager.
func MakeSeedPeerKeyInManager(clusterID uint, hostname, ip string) string {
	return MakeKeyInManager(SeedPeersNamespace, fmt.Sprintf("%d-%s-%s", clusterID, hostname, ip))
}

// MakeSchedulerKeyInManager make scheduler key in manager.
func MakeSchedulerKeyInManager(clusterID uint, hostname, ip string) string {
	return MakeKeyInManager(SchedulersNamespace, fmt.Sprintf("%d-%s-%s", clusterID, hostname, ip))
}

// MakePeerKeyInManager make peer key in manager.
func MakePeerKeyInManager(hostname, ip string) string {
	return MakeKeyInManager(PeersNamespace, fmt.Sprintf("%s-%s", hostname, ip))
}

// MakeSeedPeersKeyForPeerInManager make seed peers key for peer in manager.
func MakeSeedPeersKeyForPeerInManager(hostname, ip string) string {
	return MakeKeyInManager(PeersNamespace, fmt.Sprintf("%s-%s:seed-peers", hostname, ip))
}

// MakeSchedulersKeyForPeerInManager make schedulers key for peer in manager.
func MakeSchedulersKeyForPeerInManager(hostname, ip, version string) string {
	return MakeKeyInManager(PeersNamespace, fmt.Sprintf("%s-%s-%s:schedulers", hostname, ip, version))
}

// MakeSchedulerClusterKeyInManager make distributed rate limiter key in manager.
func MakeDistributedRateLimiterKeyInManager(key string) string {
	return MakeKeyInManager(RateLimitersNamespace, key)
}

// MakeSchedulerClusterKeyInManager make locker key of distributed rate limiter in manager.
func MakeDistributedRateLimiterLockerKeyInManager(key string) string {
	return MakeKeyInManager(RateLimitersNamespace, fmt.Sprintf("%s-lock", key))
}

// MakeApplicationsKeyInManager make applications key in manager.
func MakeApplicationsKeyInManager() string {
	return MakeNamespaceKeyInManager(ApplicationsNamespace)
}

// MakeNamespaceKeyInScheduler make namespace key in scheduler.
func MakeNamespaceKeyInScheduler(namespace string) string {
	return fmt.Sprintf("%s:%s", types.SchedulerName, namespace)
}

// MakeKeyInScheduler make key in scheduler.
func MakeKeyInScheduler(namespace, id string) string {
	return fmt.Sprintf("%s:%s", MakeNamespaceKeyInScheduler(namespace), id)
}

// MakeSchedulerClusterKeyInManager make scheduler cluster key in manager.
func MakePersistentCacheTaskKeyInScheduler(schedulerClusterID uint, taskID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s", schedulerClusterID, PersistentCacheTasksNamespace, taskID))
}

// MakePersistentCacheTasksInScheduler make persistent cache tasks in scheduler.
func MakePersistentCacheTasksInScheduler(schedulerClusterID uint) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s", schedulerClusterID, PersistentCacheTasksNamespace))
}

// MakePersistentCachePeersOfPersistentCacheTaskInScheduler make persistent cache peers of persistent cache task in scheduler.
func MakePersistentCachePeersOfPersistentCacheTaskInScheduler(schedulerClusterID uint, taskID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s:%s", schedulerClusterID, PersistentCacheTasksNamespace, taskID, PersistentCachePeersNamespace))
}

// MakePersistentPeersOfPersistentCacheTaskInScheduler make persistent peers of persistent cache task in scheduler.
func MakePersistentPeersOfPersistentCacheTaskInScheduler(schedulerClusterID uint, taskID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s:%s", schedulerClusterID, PersistentCacheTasksNamespace, taskID, PersistentPeersNamespace))
}

// MakePersistentCachePeerKeyInScheduler make persistent cache peer key in scheduler.
func MakePersistentCachePeerKeyInScheduler(schedulerClusterID uint, peerID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s", schedulerClusterID, PersistentCachePeersNamespace, peerID))
}

// MakePersistentCachePeersInScheduler make persistent cache peers in scheduler.
func MakePersistentCachePeersInScheduler(schedulerClusterID uint) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s", schedulerClusterID, PersistentCachePeersNamespace))
}

// MakePersistentCacheHostKeyInScheduler make persistent cache host key in scheduler.
func MakePersistentCacheHostKeyInScheduler(schedulerClusterID uint, hostID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s", schedulerClusterID, PersistentCacheHostsNamespace, hostID))
}

// MakePersistentCacheHostsInScheduler make persistent cache hosts in scheduler.
func MakePersistentCacheHostsInScheduler(schedulerClusterID uint) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s", schedulerClusterID, PersistentCacheHostsNamespace))
}

// MakePersistentCachePeersOfPersistentCacheHostInScheduler make persistent cache peers of persistent cache host in scheduler.
func MakePersistentCachePeersOfPersistentCacheHostInScheduler(schedulerClusterID uint, hostID string) string {
	return MakeKeyInScheduler(SchedulerClustersNamespace, fmt.Sprintf("%d:%s:%s:%s", schedulerClusterID, PersistentCacheHostsNamespace, hostID, PersistentCachePeersNamespace))
}
