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

package model

import (
	"d7y.io/dragonfly/v2/scheduler/resource"
	"encoding/json"
	"go.uber.org/atomic"
	"time"
)

type Probe struct {
	Host      *resource.Host `json:"host"`
	RTT       time.Duration  `json:"rtt"`
	CreatedAt time.Time      `json:"createdAt"`
}

type Probes struct {
	Items      []Probe          `json:"items"`
	AverageRTT *atomic.Duration `json:"averageRTT"`
	CreatedAt  *atomic.Time     `json:"createdAt"`
	UpdatedAt  *atomic.Time     `json:"updatedAt"`
}

func (p Probes) MarshalBinary() (data []byte, err error) {
	return json.Marshal(p)
}

func (p Probes) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, p)
}
