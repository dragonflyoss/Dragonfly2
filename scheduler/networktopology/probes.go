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

//go:generate mockgen -destination mocks/probes_mock.go -source probes.go -package mocks

package networktopology

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/cache"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// defaultMovingAverageWeight is the default weight of moving average.
	defaultMovingAverageWeight = 0.1
)

// Probe is the probe metadata.
type Probe struct {
	// Host metadata.
	Host *resource.Host `json:"host"`

	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration `json:"rtt"`

	// CreatedAt is the time to create probe.
	CreatedAt time.Time `json:"createdAt"`
}

// Probes is the interface to store probes.
type Probes interface {
	// Peek returns the oldest probe without removing it.
	Peek() (*Probe, error)

	// Enqueue enqueues probe into the queue.
	Enqueue(*Probe) error

	// Len gets the length of probes.
	Len() (int64, error)

	// CreatedAt is the creation time of probes.
	CreatedAt() (time.Time, error)

	// UpdatedAt is the updated time to store probe.
	UpdatedAt() (time.Time, error)

	// AverageRTT is the moving average round-trip time of probes.
	AverageRTT() (time.Duration, error)
}

// probes is the implementation of Probes.
type probes struct {
	// config is the probe config.
	config config.NetworkTopologyConfig

	// rdb is redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache cache.Cache

	// srcHostID is the source host id.
	srcHostID string

	// destHostID is the destination host id.
	destHostID string
}

// NewProbes creates a probes interface.
func NewProbes(cfg config.NetworkTopologyConfig, rdb redis.UniversalClient, cache cache.Cache, srcHostID string, destHostID string) Probes {
	return &probes{
		config:     cfg,
		rdb:        rdb,
		cache:      cache,
		srcHostID:  srcHostID,
		destHostID: destHostID,
	}
}

// Peek returns the oldest probe without removing it.
func (p *probes) Peek() (*Probe, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	probesKey := pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID)
	if cache, _, ok := p.cache.GetWithExpiration(probesKey); ok {
		probes, ok := cache.([]*Probe)
		if !ok {
			return nil, errors.New("get probes failed")
		}

		if len(probes) == 0 {
			return nil, errors.New("probes cache is empty")
		}

		return probes[0], nil
	}

	rawProbes, err := p.rdb.LRange(ctx, pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID), 0, -1).Result()
	if err != nil {
		logger.Errorf("get probes failed: %s", err.Error())
		return nil, err
	}

	var probes []*Probe
	for _, rawProbe := range rawProbes {
		probe := &Probe{}
		if err = json.Unmarshal([]byte(rawProbe), probe); err != nil {
			return nil, err
		}
		probes = append(probes, probe)
	}

	// Add cache data.
	p.cache.Set(probesKey, probes, p.config.Cache.TTL)

	return probes[0], nil
}

// Enqueue enqueues probe into the queue.
func (p *probes) Enqueue(probe *Probe) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	// Get the length of the queue.
	length, err := p.Len()
	if err != nil {
		return err
	}

	// If the queue is full, remove the oldest probe.
	if length >= int64(p.config.Probe.QueueLength) {
		if _, err := p.dequeue(); err != nil {
			return err
		}
	}

	// Push the probe into the queue.
	data, err := json.Marshal(probe)
	if err != nil {
		return err
	}

	probesKey := pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID)
	if err := p.rdb.RPush(ctx, probesKey, data).Err(); err != nil {
		return err
	}
	p.cache.Delete(probesKey)

	// Calculate the moving average round-trip time.
	var averageRTT time.Duration
	if length > 0 {
		// If the queue is not empty, calculate the
		// moving average round-trip time.
		rawProbes, err := p.rdb.LRange(context.Background(), probesKey, 0, -1).Result()
		if err != nil {
			return err
		}

		for index, rawProbe := range rawProbes {
			probe := &Probe{}
			if err = json.Unmarshal([]byte(rawProbe), probe); err != nil {
				return err
			}

			if index == 0 {
				averageRTT = probe.RTT
				continue
			}

			averageRTT = time.Duration(float64(averageRTT)*defaultMovingAverageWeight +
				float64(probe.RTT)*(1-defaultMovingAverageWeight))
		}
	} else {
		// If the queue is empty, use the probe round-trip time as
		// the moving average round-trip time.
		averageRTT = probe.RTT
	}

	// Update the moving average round-trip time and updated time.
	networkTopologyKey := pkgredis.MakeNetworkTopologyKeyInScheduler(p.srcHostID, p.destHostID)
	if err := p.rdb.HSet(ctx, networkTopologyKey, "averageRTT", averageRTT.Nanoseconds()).Err(); err != nil {
		return err
	}
	if err := p.rdb.HSet(ctx, networkTopologyKey, "updatedAt", probe.CreatedAt.Format(time.RFC3339Nano)).Err(); err != nil {
		return err
	}
	p.cache.Delete(networkTopologyKey)

	probedCountKey := pkgredis.MakeProbedCountKeyInScheduler(p.destHostID)
	if err := p.rdb.Incr(ctx, probedCountKey).Err(); err != nil {
		return err
	}
	p.cache.Delete(probedCountKey)

	return nil
}

// Length gets the length of probes.
func (p *probes) Len() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	probesKey := pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID)
	if cache, _, ok := p.cache.GetWithExpiration(probesKey); ok {
		probes, ok := cache.([]*Probe)
		if !ok {
			return int64(0), errors.New("get probes failed")
		}

		return int64(len(probes)), nil
	}

	rawProbes, err := p.rdb.LRange(ctx, pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID), 0, -1).Result()
	if err != nil {
		logger.Errorf("get probes failed: %s", err.Error())
		return int64(0), err
	}

	if len(rawProbes) == 0 {
		return int64(0), err
	}

	var probes []*Probe
	for _, rawProbe := range rawProbes {
		probe := &Probe{}
		if err = json.Unmarshal([]byte(rawProbe), probe); err != nil {
			return int64(0), err
		}
		probes = append(probes, probe)
	}

	// Add cache data.
	p.cache.Set(probesKey, probes, p.config.Cache.TTL)

	return int64(len(probes)), nil
}

// CreatedAt is the creation time of probes.
func (p *probes) CreatedAt() (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	var networkTopology map[string]string
	networkTopologyKey := pkgredis.MakeNetworkTopologyKeyInScheduler(p.srcHostID, p.destHostID)
	cache, _, ok := p.cache.GetWithExpiration(networkTopologyKey)
	if ok {
		if networkTopology, ok = cache.(map[string]string); !ok {
			return time.Time{}, errors.New("get networkTopology failed")
		}
	} else {
		var err error
		if networkTopology, err = p.rdb.HGetAll(ctx, networkTopologyKey).Result(); err != nil {
			return time.Time{}, err
		}

		// Add cache data.
		p.cache.Set(networkTopologyKey, networkTopology, p.config.Cache.TTL)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, networkTopology["createdAt"])
	if err != nil {
		return time.Time{}, err
	}

	return createdAt, nil
}

// UpdatedAt is the updated time to store probe.
func (p *probes) UpdatedAt() (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	var networkTopology map[string]string
	networkTopologyKey := pkgredis.MakeNetworkTopologyKeyInScheduler(p.srcHostID, p.destHostID)
	cache, _, ok := p.cache.GetWithExpiration(networkTopologyKey)
	if ok {
		if networkTopology, ok = cache.(map[string]string); !ok {
			return time.Time{}, errors.New("get networkTopology failed")
		}
	} else {
		var err error
		if networkTopology, err = p.rdb.HGetAll(ctx, networkTopologyKey).Result(); err != nil {
			return time.Time{}, err
		}

		// Add cache data.
		p.cache.Set(networkTopologyKey, networkTopology, p.config.Cache.TTL)
	}

	updatedAt, err := time.Parse(time.RFC3339Nano, networkTopology["updatedAt"])
	if err != nil {
		return time.Time{}, err
	}

	return updatedAt, nil
}

// AverageRTT is the moving average round-trip time of probes.
func (p *probes) AverageRTT() (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	var networkTopology map[string]string
	networkTopologyKey := pkgredis.MakeNetworkTopologyKeyInScheduler(p.srcHostID, p.destHostID)
	cache, _, ok := p.cache.GetWithExpiration(networkTopologyKey)
	if ok {
		if networkTopology, ok = cache.(map[string]string); !ok {
			return time.Duration(0), errors.New("get networkTopology failed")
		}
	} else {
		var err error
		if networkTopology, err = p.rdb.HGetAll(ctx, networkTopologyKey).Result(); err != nil {
			return time.Duration(0), err
		}

		// Add cache data.
		p.cache.Set(networkTopologyKey, networkTopology, p.config.Cache.TTL)
	}

	averageRTT, err := strconv.ParseInt(networkTopology["averageRTT"], 10, 64)
	if err != nil {
		return time.Duration(0), err
	}

	return time.Duration(averageRTT), nil
}

// dequeue removes and returns the oldest probe.
func (p *probes) dequeue() (*Probe, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	probesKey := pkgredis.MakeProbesKeyInScheduler(p.srcHostID, p.destHostID)
	rawProbe, err := p.rdb.LPop(ctx, probesKey).Bytes()
	if err != nil {
		return nil, err
	}
	p.cache.Delete(probesKey)

	probe := &Probe{}
	if err = json.Unmarshal(rawProbe, probe); err != nil {
		return nil, err
	}

	return probe, nil
}
