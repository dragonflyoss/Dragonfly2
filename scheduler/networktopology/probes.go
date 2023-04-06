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
	"container/list"
	"errors"
	"sync"
	"time"

	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// DefaultMovingAverageWeight is the weight of the moving average.
	DefaultMovingAverageWeight = 0.1
)

type Probe struct {
	// Host metadata.
	Host *resource.Host

	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration

	// CreatedAt is the time to create probe.
	CreatedAt time.Time
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

	// Items returns the probes list.
	Items() *list.List

	// Length gets the length of probes.
	Length() int

	// CreatedAt is the creation time of probes.
	CreatedAt() time.Time

	// UpdatedAt is the updated time to store probe.
	UpdatedAt() time.Time

	// AverageRTT is the average round-trip time of probes.
	AverageRTT() time.Duration
}

type probes struct {
	// host stores host metadata.
	host *resource.Host

	// limit is the length limit of probe queue.
	limit int

	// items are the list of probe.
	items *list.List

	// averageRTT is the average round-trip time of probes.
	averageRTT *atomic.Duration

	// createdAt is the creation time of probes.
	createdAt *atomic.Time

	// updatedAt is the update time to store probe.
	updatedAt *atomic.Time

	// mu locks for probe queue.
	mu *sync.RWMutex
}

// NewProbes creates a new probe list instance.
func NewProbes(limit int, host *resource.Host) Probes {
	return &probes{
		limit:      limit,
		host:       host,
		items:      list.New(),
		averageRTT: atomic.NewDuration(0),
		createdAt:  atomic.NewTime(time.Now()),
		updatedAt:  atomic.NewTime(time.Time{}),
		mu:         &sync.RWMutex{},
	}
}

// Peek returns the oldest probe without removing it.
func (p *probes) Peek() (*Probe, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.items.Len() == 0 {
		return nil, false
	}

	probe, ok := p.items.Front().Value.(*Probe)
	if !ok {
		return nil, false
	}

	return probe, true
}

// Enqueue enqueues probe into the queue.
func (p *probes) Enqueue(probe *Probe) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove the oldest probe if the queue is full.
	if p.items.Len() == p.limit {
		front := p.items.Front()
		p.items.Remove(front)
	}
	p.items.PushBack(probe)

	// Calculate the average RTT.
	for e := p.items.Front(); e != nil; e = e.Next() {
		probe, ok := e.Value.(*Probe)
		if !ok {
			return errors.New("invalid probe")
		}

		if p.items.Len() == 1 {
			p.averageRTT.Store(probe.RTT)
			continue
		}

		p.averageRTT.Store(time.Duration(float64(p.averageRTT.Load())*DefaultMovingAverageWeight +
			float64(probe.RTT)*(1-DefaultMovingAverageWeight)))
	}

	p.updatedAt = atomic.NewTime(probe.CreatedAt)
	return nil
}

// Dequeue removes and returns the oldest probe.
func (p *probes) Dequeue() (*Probe, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.items.Len() == 0 {
		return nil, false
	}

	probe, ok := p.items.Front().Value.(*Probe)
	if !ok {
		return nil, false
	}

	p.items.Remove(p.items.Front())
	return probe, true
}

// Items returns the probes list.
func (p *probes) Items() *list.List {
	return p.items
}

// Length gets the length of probes.
func (p *probes) Length() int {
	return p.items.Len()
}

// CreatedAt is the creation time of probes.
func (p *probes) CreatedAt() time.Time {
	return p.createdAt.Load()
}

// UpdatedAt is the updated time to store probe.
func (p *probes) UpdatedAt() time.Time {
	return p.updatedAt.Load()
}

// AverageRTT is the average round-trip time of probes.
func (p *probes) AverageRTT() time.Duration {
	return p.averageRTT.Load()
}
