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
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// DefaultMovingAverageWeight is the weight of the moving average.
	DefaultMovingAverageWeight = 0.1
)

type Probe struct {
	// Host metadata.
	Host *resource.Host `json:"host"`

	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration `json:"rtt"`

	// CreatedAt is the time to create probe.
	CreatedAt time.Time `json:"createdAt"`
}

// NewProbe creates a new probe instance.
func NewProbe(host *resource.Host, rtt time.Duration, createdAt time.Time) *Probe {
	return &Probe{
		Host:      host,
		RTT:       rtt,
		CreatedAt: createdAt,
	}
}

type Probes interface {
	// Peek returns the oldest probe without removing it.
	Peek() (*Probe, bool)

	// Enqueue enqueues probe into the queue.
	Enqueue(*Probe) error

	// Dequeue removes and returns the oldest probe.
	Dequeue() (*Probe, bool)

	// Length gets the length of probes.
	Length() int64

	// CreatedAt is the creation time of probes.
	CreatedAt() time.Time

	// UpdatedAt is the updated time to store probe.
	UpdatedAt() time.Time

	// AverageRTT is the average round-trip time of probes.
	AverageRTT() time.Duration
}

type probes struct {
	// Redis universal client interface.
	rdb redis.UniversalClient

	// limit is the length limit of probe queue.
	limit int

	// src is the source host id.
	src string

	// dest is the destination host id.
	dest string
}

// NewProbes creates a probes interface.
func NewProbes(rdb redis.UniversalClient, limit int, src string, dest string) Probes {
	return &probes{
		rdb:   rdb,
		limit: limit,
		src:   src,
		dest:  dest,
	}
}

// Peek returns the oldest probe without removing it.
func (p *probes) Peek() (*Probe, bool) {
	key := fmt.Sprintf("probes:%s:%s", p.src, p.dest)
	str, err := p.rdb.LIndex(context.Background(), key, 0).Result()
	if err != nil {
		return nil, false
	}

	probe := &Probe{}
	if err = json.Unmarshal([]byte(str), probe); err != nil {
		return nil, false
	}

	return probe, true
}

// Enqueue enqueues probe into the queue.
func (p *probes) Enqueue(probe *Probe) error {
	length := p.Length()
	if length == int64(p.limit) {
		if _, ok := p.Dequeue(); !ok {
			return errors.New("remove the oldest probe error")
		}
	}

	probesKey := fmt.Sprintf("probes:%s:%s", p.src, p.dest)
	data, err := json.Marshal(probe)
	if err != nil {
		return err
	}

	if err = p.rdb.RPush(context.Background(), probesKey, data).Err(); err != nil {
		return err
	}

	networkTopologyKey := fmt.Sprintf("network-topology:%s:%s", p.src, p.dest)
	if length == 0 {
		if _, err := p.rdb.Pipelined(context.Background(), func(rdb redis.Pipeliner) error {
			rdb.HSet(context.Background(), networkTopologyKey, "averageRTT", probe.RTT.Nanoseconds())
			rdb.HSet(context.Background(), networkTopologyKey, "createdAt", probe.CreatedAt.UnixNano())
			rdb.HSet(context.Background(), networkTopologyKey, "updatedAt", probe.CreatedAt.UnixNano())
			return nil
		}); err != nil {
			return err
		}

		return nil
	}
	values, err := p.rdb.LRange(context.Background(), probesKey, 0, -1).Result()
	if err != nil {
		return err
	}

	var averageRTT time.Duration
	for _, value := range values {
		probe := &Probe{}
		if err = json.Unmarshal([]byte(value), probe); err != nil {
			return err
		}

		averageRTT = time.Duration(float64(averageRTT)*DefaultMovingAverageWeight +
			float64(probe.RTT)*(1-DefaultMovingAverageWeight))
	}

	if _, err := p.rdb.Pipelined(context.Background(), func(rdb redis.Pipeliner) error {
		rdb.HSet(context.Background(), networkTopologyKey, "averageRTT", averageRTT.Nanoseconds())
		rdb.HSet(context.Background(), networkTopologyKey, "updatedAt", probe.CreatedAt.UnixNano())
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Dequeue removes and returns the oldest probe.
func (p *probes) Dequeue() (*Probe, bool) {
	key := fmt.Sprintf("probes:%s:%s", p.src, p.dest)
	str, err := p.rdb.LPop(context.Background(), key).Result()
	if err != nil {
		return nil, false
	}

	probe := &Probe{}
	if err = json.Unmarshal([]byte(str), probe); err != nil {
		return nil, false
	}

	return probe, true
}

// Length gets the length of probes.
func (p *probes) Length() int64 {
	key := fmt.Sprintf("probes:%s:%s", p.src, p.dest)
	length, err := p.rdb.LLen(context.Background(), key).Result()
	if err != nil {
		return 0
	}

	return length
}

// CreatedAt is the creation time of probes.
func (p *probes) CreatedAt() time.Time {
	key := fmt.Sprintf("network-topology:%s:%s", p.src, p.dest)
	value, err := p.rdb.HGet(context.Background(), key, "createdAt").Result()
	if err != nil {
		return time.Time{}
	}

	nano, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, nano)
}

// UpdatedAt is the updated time to store probe.
func (p *probes) UpdatedAt() time.Time {
	key := fmt.Sprintf("network-topology:%s:%s", p.src, p.dest)
	value, err := p.rdb.HGet(context.Background(), key, "updatedAt").Result()
	if err != nil {
		return time.Time{}
	}

	nano, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, nano)
}

// AverageRTT is the average round-trip time of probes.
func (p *probes) AverageRTT() time.Duration {
	key := fmt.Sprintf("network-topology:%s:%s", p.src, p.dest)
	value, err := p.rdb.HGet(context.Background(), key, "averageRTT").Result()
	if err != nil {
		return time.Duration(0)
	}

	nano, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Duration(0)
	}

	return time.Duration(nano)
}
