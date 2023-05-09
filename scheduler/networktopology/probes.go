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

package networktopology

import (
	"time"

	"d7y.io/dragonfly/v2/scheduler/resource"
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

type Probes struct {
	// AverageRTT is the average round-trip time of probes.
	AverageRTT time.Duration

	// CreatedAt is the creation time of probes.
	CreatedAt time.Time

	// UpdatedAt is the update time to store probe.
	UpdatedAt time.Time
}

// NewProbes creates a new probe list instance.
func NewProbes() *Probes {
	return &Probes{
		AverageRTT: 0,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Time{},
	}
}
