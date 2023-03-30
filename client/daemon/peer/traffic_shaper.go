/*
 *     Copyright 2022 The Dragonfly Authors
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

package peer

import (
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/math"
)

const (
	TypePlainTrafficShaper    = "plain"
	TypeSamplingTrafficShaper = "sampling"
)

// TrafficShaper allocates bandwidth for running tasks dynamically
type TrafficShaper interface {
	// Start starts the TrafficShaper
	Start()
	// Stop stops the TrafficShaper
	Stop()
	// AddTask starts managing the new task
	AddTask(taskID string, ptc *peerTaskConductor)
	// RemoveTask removes completed task
	RemoveTask(taskID string)
	// Record records task's used bandwidth
	Record(taskID string, n int)
	// GetBandwidth gets the total download bandwidth in the past second
	GetBandwidth() int64
}

func NewTrafficShaper(trafficShaperType string, totalRateLimit rate.Limit, computePieceSize func(int64) uint32) TrafficShaper {
	var ts TrafficShaper
	switch trafficShaperType {
	case TypeSamplingTrafficShaper:
		ts = NewSamplingTrafficShaper(totalRateLimit, computePieceSize)
	case TypePlainTrafficShaper:
		ts = NewPlainTrafficShaper()
	default:
		logger.Warnf("type \"%s\" doesn't exist, use plain traffic shaper instead", trafficShaperType)
		ts = NewPlainTrafficShaper()
	}
	return ts
}

type plainTrafficShaper struct {
	// total used bandwidth in the past second
	lastSecondBandwidth *atomic.Int64
	// total used bandwidth in the current second
	usingBandWidth *atomic.Int64
	stopCh         chan struct{}
}

func NewPlainTrafficShaper() TrafficShaper {
	return &plainTrafficShaper{
		lastSecondBandwidth: atomic.NewInt64(0),
		usingBandWidth:      atomic.NewInt64(0),
		stopCh:              make(chan struct{}),
	}
}

func (ts *plainTrafficShaper) Start() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ts.lastSecondBandwidth.Store(ts.usingBandWidth.Load())
				ts.usingBandWidth.Store(0)
			case <-ts.stopCh:
				return
			}
		}
	}()
}

func (ts *plainTrafficShaper) Stop() {
	close(ts.stopCh)
}

func (ts *plainTrafficShaper) AddTask(_ string, _ *peerTaskConductor) {
}

func (ts *plainTrafficShaper) RemoveTask(_ string) {
}

func (ts *plainTrafficShaper) Record(_ string, n int) {
	ts.usingBandWidth.Add(int64(n))
}

func (ts *plainTrafficShaper) GetBandwidth() int64 {
	return ts.lastSecondBandwidth.Load()
}

type taskEntry struct {
	ptc       *peerTaskConductor
	pieceSize uint32
	// used bandwidth in the past second
	lastSecondBandwidth *atomic.Int64
	// need bandwidth in the next second
	needBandwidth int64
	// indicates if the bandwidth need to be updated, tasks added within one second don't need to be updated
	needUpdate bool
}

type samplingTrafficShaper struct {
	*logger.SugaredLoggerOnWith
	sync.RWMutex
	computePieceSize func(int64) uint32
	totalRateLimit   rate.Limit
	// total used bandwidth in the past second
	lastSecondBandwidth *atomic.Int64
	// total used bandwidth in the current second
	usingBandWidth *atomic.Int64
	tasks          map[string]*taskEntry
	stopCh         chan struct{}
}

func NewSamplingTrafficShaper(totalRateLimit rate.Limit, computePieceSize func(int64) uint32) TrafficShaper {
	log := logger.With("component", "TrafficShaper")
	return &samplingTrafficShaper{
		SugaredLoggerOnWith: log,
		computePieceSize:    computePieceSize,
		totalRateLimit:      totalRateLimit,
		lastSecondBandwidth: atomic.NewInt64(0),
		usingBandWidth:      atomic.NewInt64(0),
		tasks:               make(map[string]*taskEntry),
		stopCh:              make(chan struct{}),
	}
}

func (ts *samplingTrafficShaper) Start() {
	go func() {
		// update bandwidth of all running tasks every second
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ts.lastSecondBandwidth.Store(ts.usingBandWidth.Load())
				ts.usingBandWidth.Store(0)
				ts.updateLimit()
			case <-ts.stopCh:
				return
			}
		}
	}()
}

func (ts *samplingTrafficShaper) Stop() {
	close(ts.stopCh)
}

// updateLimit updates every task's limit every second
func (ts *samplingTrafficShaper) updateLimit() {
	var totalNeedBandwidth int64
	var totalLeastBandwidth int64
	ts.RLock()
	defer ts.RUnlock()
	// compute overall remaining length of all tasks
	for _, te := range ts.tasks {
		oldLimit := int64(te.ptc.limiter.Limit())
		needBandwidth := te.lastSecondBandwidth.Swap(0)
		if !te.needUpdate {
			// if this task is added within 1 second, don't reduce its limit this time
			te.needUpdate = true
			needBandwidth = math.Max(needBandwidth, oldLimit)
		}
		if contentLength := te.ptc.contentLength.Load(); contentLength > 0 {
			remainingLength := contentLength - te.ptc.completedLength.Load()
			needBandwidth = math.Min(remainingLength, needBandwidth)
		}
		// delta bandwidth, make sure it's larger than 0
		needBandwidth = math.Max(needBandwidth-int64(te.pieceSize), int64(0))
		te.needBandwidth = needBandwidth
		totalNeedBandwidth += needBandwidth
		totalLeastBandwidth += int64(te.pieceSize)
	}

	// allocate delta bandwidth for tasks
	for _, te := range ts.tasks {
		// diffLimit indicates the difference between the allocated bandwidth and pieceSize
		var diffLimit float64
		// make sure new limit is not smaller than pieceSize
		diffLimit = math.Max(
			(float64(ts.totalRateLimit)-float64(totalLeastBandwidth))*(float64(te.needBandwidth)/float64(totalNeedBandwidth)), 0)
		te.ptc.limiter.SetLimit(rate.Limit(diffLimit + float64(te.pieceSize)))
		ts.Debugf("period update limit, task %s, need bandwidth %d, diff rate limit %f", te.ptc.taskID, te.needBandwidth, diffLimit)
	}
}

func (ts *samplingTrafficShaper) AddTask(taskID string, ptc *peerTaskConductor) {
	ts.Lock()
	defer ts.Unlock()
	nTasks := len(ts.tasks)
	if nTasks == 0 {
		nTasks++
	}
	pieceSize := ts.computePieceSize(ptc.contentLength.Load())
	limit := rate.Limit(math.Max(float64(ts.totalRateLimit)/float64(nTasks), float64(pieceSize)))
	// make sure bandwidth is not smaller than pieceSize
	ptc.limiter.SetLimit(limit)
	ts.tasks[taskID] = &taskEntry{ptc: ptc, lastSecondBandwidth: atomic.NewInt64(0), pieceSize: pieceSize}
	var totalNeedRateLimit rate.Limit
	for _, te := range ts.tasks {
		totalNeedRateLimit += te.ptc.limiter.Limit()
	}
	ratio := ts.totalRateLimit / totalNeedRateLimit
	// reduce all running tasks' bandwidth
	for _, te := range ts.tasks {
		// make sure bandwidth is not smaller than pieceSize
		newLimit := math.Max(ratio*te.ptc.limiter.Limit(), rate.Limit(te.pieceSize))
		te.ptc.limiter.SetLimit(newLimit)
		ts.Debugf("a task added, task %s rate limit updated to %f", te.ptc.taskID, newLimit)
	}
}

func (ts *samplingTrafficShaper) RemoveTask(taskID string) {
	ts.Lock()
	defer ts.Unlock()

	var limit rate.Limit
	if task, ok := ts.tasks[taskID]; ok {
		limit = task.ptc.limiter.Limit()
	} else {
		ts.Debugf("the task %s is already removed", taskID)
		return
	}

	delete(ts.tasks, taskID)
	ratio := ts.totalRateLimit / (ts.totalRateLimit - limit)
	// increase all running tasks' bandwidth
	for _, te := range ts.tasks {
		newLimit := ratio * te.ptc.limiter.Limit()
		te.ptc.limiter.SetLimit(newLimit)
		ts.Debugf("a task removed, task %s rate limit updated to %f", te.ptc.taskID, newLimit)
	}
}

func (ts *samplingTrafficShaper) Record(taskID string, n int) {
	ts.usingBandWidth.Add(int64(n))
	ts.RLock()
	if task, ok := ts.tasks[taskID]; ok {
		task.lastSecondBandwidth.Add(int64(n))
	} else {
		ts.Warnf("the task %s is not found when record it", taskID)
	}
	ts.RUnlock()
}

func (ts *samplingTrafficShaper) GetBandwidth() int64 {
	return ts.lastSecondBandwidth.Load()
}
