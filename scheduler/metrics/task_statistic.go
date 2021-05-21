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

package metrics

import (
	"sync"
	"time"
)

type TaskStatistic struct {
	lock          sync.RWMutex
	StartTime     time.Time
	EndTime       time.Time
	PeerCount     int32
	FinishedCount int32
	CostList      []int32
}

type StatisticInfo struct {
	StartTime     time.Time
	EndTime       time.Time
	PeerCount     int32
	FinishedCount int32
	Costs         map[int32]int32
}

func (t *TaskStatistic) SetStartTime(start time.Time) {
	t.lock.Lock()
	t.StartTime = start
	t.lock.Unlock()
}

func (t *TaskStatistic) SetEndTime(end time.Time) {
	t.lock.Lock()
	t.EndTime = end
	t.lock.Unlock()
}

func (t *TaskStatistic) AddPeerTaskStart() {
	t.lock.Lock()
	t.PeerCount++
	t.lock.Unlock()
}

func (t *TaskStatistic) AddPeerTaskDown(cost int32) {
	t.lock.Lock()
	t.CostList = append(t.CostList, cost)
	t.lock.Unlock()
}

func (t *TaskStatistic) GetStatistic() (info *StatisticInfo) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	info = &StatisticInfo{
		StartTime:     t.StartTime,
		EndTime:       t.EndTime,
		PeerCount:     t.PeerCount,
		FinishedCount: t.FinishedCount,
		Costs:         make(map[int32]int32),
	}

	if info.EndTime.IsZero() {
		info.EndTime = time.Now()
	}

	count := len(t.CostList)
	count90 := count * 90 / 100
	count95 := count * 95 / 100

	totalCost := int64(0)

	for i, cost := range t.CostList {
		totalCost += int64(cost)
		switch i {
		case count90:
			info.Costs[90] = int32(totalCost / int64(count90))
		case count95:
			info.Costs[95] = int32(totalCost / int64(count95))
		}
	}
	if count > 0 {
		info.Costs[100] = int32(totalCost / int64(count))
	}

	return
}
