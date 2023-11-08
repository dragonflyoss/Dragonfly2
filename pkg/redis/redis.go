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
	"strings"

	"github.com/go-redis/redis/v8"

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

	// ApplicationsNamespace prefix of applications namespace cache key.
	ApplicationsNamespace = "applications"

	// BucketsNamespace prefix of buckets namespace cache key.
	BucketsNamespace = "buckets"

	// NetworkTopologyNamespace prefix of network topology namespace cache key.
	NetworkTopologyNamespace = "network-topology"

	// ProbesNamespace prefix of probes namespace cache key.
	ProbesNamespace = "probes"

	// ProbedCountNamespace prefix of probed count namespace cache key.
	ProbedCountNamespace = "probed-count"
)

// NewRedis returns a new redis client.
func NewRedis(cfg *redis.UniversalOptions) (redis.UniversalClient, error) {
	redis.SetLogger(&redisLogger{})
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      cfg.Addrs,
		MasterName: cfg.MasterName,
		DB:         cfg.DB,
		Username:   cfg.Username,
		Password:   cfg.Password,
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
func MakeSchedulersKeyForPeerInManager(hostname, ip string) string {
	return MakeKeyInManager(PeersNamespace, fmt.Sprintf("%s-%s:schedulers", hostname, ip))
}

// MakeApplicationsKeyInManager make applications key in manager.
func MakeApplicationsKeyInManager() string {
	return MakeNamespaceKeyInManager(ApplicationsNamespace)
}

// MakeBucketKeyInManager make bucket key in manager.
func MakeBucketKeyInManager(name string) string {
	return MakeKeyInManager(BucketsNamespace, name)
}

// MakeNamespaceKeyInScheduler make namespace key in scheduler.
func MakeNamespaceKeyInScheduler(namespace string) string {
	return fmt.Sprintf("%s:%s", types.SchedulerName, namespace)
}

// MakeKeyInScheduler make key in scheduler.
func MakeKeyInScheduler(namespace, id string) string {
	return fmt.Sprintf("%s:%s", MakeNamespaceKeyInScheduler(namespace), id)
}

// MakeNetworkTopologyKeyInScheduler make network topology key in scheduler.
func MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID string) string {
	return MakeKeyInScheduler(NetworkTopologyNamespace, fmt.Sprintf("%s:%s", srcHostID, destHostID))
}

// ParseNetworkTopologyKeyInScheduler parse network topology key in scheduler.
func ParseNetworkTopologyKeyInScheduler(key string) (string, string, string, string, error) {
	elements := strings.Split(key, KeySeparator)
	if len(elements) != 4 {
		return "", "", "", "", fmt.Errorf("invalid network topology key: %s", key)
	}

	return elements[0], elements[1], elements[2], elements[3], nil
}

// MakeProbesKeyInScheduler make probes key in scheduler.
func MakeProbesKeyInScheduler(srcHostID, destHostID string) string {
	return MakeKeyInScheduler(ProbesNamespace, fmt.Sprintf("%s:%s", srcHostID, destHostID))
}

// ParseProbedCountKeyInScheduler parse probed count key in scheduler.
func ParseProbedCountKeyInScheduler(key string) (string, string, string, error) {
	elements := strings.Split(key, KeySeparator)
	if len(elements) != 3 {
		return "", "", "", fmt.Errorf("invalid probed count key: %s", key)
	}

	return elements[0], elements[1], elements[2], nil
}

// MakeProbedCountKeyInScheduler make probed count key in scheduler.
func MakeProbedCountKeyInScheduler(hostID string) string {
	return MakeKeyInScheduler(ProbedCountNamespace, hostID)
}
