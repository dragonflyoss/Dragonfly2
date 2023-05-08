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

//go:generate mockgen -destination mocks/network_topology_mock.go -source network_topology.go -package mocks

package networktopology

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// DefaultMovingAverageWeight is the weight of the moving average.
	DefaultMovingAverageWeight = 0.1
)

type NetworkTopology interface {
	// Peek returns the oldest probe without removing it.
	Peek(src, dest string) (*Probe, bool)

	// Enqueue enqueues probe into the queue.
	Enqueue(src, dest string, probe *Probe) error

	// Dequeue removes and returns the oldest probe.
	Dequeue(src, dest string) (*Probe, bool)

	// Items returns the probes list.
	Items(src, dest string) []*Probe

	// Length gets the length of probes.
	Length(src, dest string) int64

	// CreatedAt is the creation time of probes.
	CreatedAt(src, dest string) time.Time

	// UpdatedAt is the updated time to store probe.
	UpdatedAt(src, dest string) time.Time

	// AverageRTT is the average round-trip time of probes.
	AverageRTT(src, dest string) time.Duration

	// LoadDestHosts returns parents for source.
	LoadDestHosts(src string) ([]string, bool)

	// DeleteDestHosts deletes parents for source.
	DeleteDestHosts(src string) error

	// StoreProbe stores probe between two hosts.
	StoreProbe(src, dest string, probe *Probe) bool
}

type networkTopology struct {
	// Redis universal client interface.
	rdb redis.UniversalClient

	// Scheduler config.
	config *config.Config

	// Resource interface
	resource resource.Resource

	// Storage interface
	storage storage.Storage

	// Manager client interface
	managerClient managerclient.V2
}

// New network topology interface.
func NewNetworkTopology(cfg *config.Config, resource resource.Resource, storage storage.Storage, managerClient managerclient.V2) (NetworkTopology, error) {
	rdb, err := pkgredis.NewRedis(&redis.UniversalOptions{
		Addrs:      cfg.Database.Redis.Addrs,
		MasterName: cfg.Database.Redis.MasterName,
		DB:         cfg.Database.Redis.NetworkTopologyDB,
		Username:   cfg.Database.Redis.Username,
		Password:   cfg.Database.Redis.Password,
	})
	if err != nil {
		return nil, err
	}

	return &networkTopology{
		rdb:           rdb,
		config:        cfg,
		resource:      resource,
		storage:       storage,
		managerClient: managerClient,
	}, nil
}

// Peek returns the oldest probe without removing it.
func (n *networkTopology) Peek(src, dest string) (*Probe, bool) {
	probe := &Probe{}
	jsonStr, err := n.rdb.LIndex(context.Background(), "probes:"+src+":"+dest, 0).Result()
	if err != nil {
		return nil, false
	}

	err = probe.UnmarshalBinary([]byte(jsonStr))
	if err != nil {
		return nil, false
	}

	return probe, true
}

// Enqueue enqueues probe into the queue.
func (n *networkTopology) Enqueue(src, dest string, probe *Probe) error {
	err := n.rdb.RPush(context.Background(), "probes:"+src+":"+dest, probe).Err()
	if err != nil {
		return err
	}

	return nil
}

// Dequeue removes and returns the oldest probe.
func (n *networkTopology) Dequeue(src, dest string) (*Probe, bool) {
	probe := &Probe{}
	jsonStr, err := n.rdb.LPop(context.Background(), "probes:"+src+":"+dest).Result()
	if err != nil {
		return nil, false
	}

	err = probe.UnmarshalBinary([]byte(jsonStr))
	if err != nil {
		return nil, false
	}

	return probe, true
}

// Items returns the probes list.
func (n *networkTopology) Items(src, dest string) []*Probe {
	probes := make([]*Probe, 0)
	for {
		probe := &Probe{}
		jsonStr, err := n.rdb.LPop(context.Background(), "probes:"+src+":"+dest).Result()
		if err == redis.Nil {
			break
		}

		err = probe.UnmarshalBinary([]byte(jsonStr))
		if err != nil {
			return probes
		}

		probes = append(probes, probe)
	}
	return probes
}

// Length gets the length of probes.
func (n *networkTopology) Length(src, dest string) int64 {
	length, err := n.rdb.LLen(context.Background(), "probes:"+src+":"+dest).Result()
	if err != nil {
		return 0
	}

	return length
}

// CreatedAt is the creation time of probes.
func (n *networkTopology) CreatedAt(src, dest string) time.Time {
	value, err := n.rdb.HGet(context.Background(), "network-topology:"+src+":"+dest, "createdAt").Result()
	if err != nil {
		return time.Time{}
	}

	createdAt, err := time.Parse(http.TimeFormat, value)
	if err != nil {
		return time.Time{}
	}

	return createdAt
}

// UpdatedAt is the updated time to store probe.
func (n *networkTopology) UpdatedAt(src, dest string) time.Time {
	value, err := n.rdb.HGet(context.Background(), "network-topology"+src+":"+dest, "updatedAt").Result()
	if err != nil {
		return time.Time{}
	}

	updatedAt, err := time.Parse(http.TimeFormat, value)
	if err != nil {
		return time.Time{}
	}

	return updatedAt
}

// AverageRTT is the average round-trip time of probes.
func (n *networkTopology) AverageRTT(src, dest string) time.Duration {
	value, err := n.rdb.HGet(context.Background(), "network-topology"+src+":"+dest, "averageRTT").Result()
	if err != nil {
		return 0
	}

	averageRTT, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}

	return averageRTT
}

// LoadDestHosts returns destination hosts for source.
func (n *networkTopology) LoadDestHosts(src string) ([]string, bool) {
	str := "network-topology:" + src + ":*"
	keys, err := n.rdb.Keys(context.Background(), str).Result()
	if err != nil {
		return []string{}, false
	}

	destHosts := make([]string, 0)
	for _, key := range keys {
		destHosts = append(destHosts, key[len(str):])
	}

	return destHosts, true
}

// DeleteDestHosts deletes destination hosts for source.
func (n *networkTopology) DeleteDestHosts(src string) error {
	err := n.rdb.Del(context.Background(), "network-topology:"+src+":*").Err()
	if err != nil {
		return err
	}

	err = n.rdb.Del(context.Background(), "probes:"+src+":*").Err()
	if err != nil {
		return err
	}

	return nil
}

// StoreProbe stores probe between two hosts.
func (n *networkTopology) StoreProbe(src, dest string, probe *Probe) bool {

	// Update probe list
	length := n.Length(src, dest)
	if length == config.DefaultProbeQueueLength {
		err := n.Enqueue(src, dest, probe)
		if err != nil {
			return false
		}
	}

	_, ok := n.Dequeue(src, dest)
	if ok != false {
		return false
	}

	// Update probes struct
	key := "network-topology:" + src + ":" + "dest"
	if length == 0 {
		if _, err := n.rdb.Pipelined(context.Background(), func(rdb redis.Pipeliner) error {
			rdb.HSet(context.Background(), key, "length", probe.RTT)
			rdb.HSet(context.Background(), key, "averageRTT", probe.RTT)
			rdb.HSet(context.Background(), key, "createdAt", time.Now())
			rdb.HSet(context.Background(), key, "updatedAt", probe.CreatedAt)
			return nil
		}); err != nil {
			return false
		}
	} else {
		value, err := n.rdb.HGet(context.Background(), key, "averageRTT").Result()
		if err != nil {
			return false
		}

		averageRtt, err := strconv.ParseFloat(value, 64)
		if _, err := n.rdb.Pipelined(context.Background(), func(rdb redis.Pipeliner) error {
			rdb.HSet(context.Background(), key, "averageRTT", float64(averageRtt)*DefaultMovingAverageWeight+
				float64(probe.RTT)*(1-DefaultMovingAverageWeight))
			rdb.HSet(context.Background(), key, "updatedAt", probe.CreatedAt)
			return nil
		}); err != nil {
			return false
		}
	}

	return true
}
