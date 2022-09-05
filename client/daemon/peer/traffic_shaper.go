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
}

func NewTrafficShaper(totalRateLimit rate.Limit, trafficShaperType string, computePieceSize func(int64) uint32) TrafficShaper {
	var ts TrafficShaper
	switch trafficShaperType {
	case TypeSamplingTrafficShaper:
		ts = NewSamplingTrafficShaper(totalRateLimit, computePieceSize)
	case TypePlainTrafficShaper:
		ts = NewPlainTrafficShaper()
	default:
		ts = NewPlainTrafficShaper()
	}
	return ts
}

type plainTrafficShaper struct {
}

func NewPlainTrafficShaper() TrafficShaper {
	return &plainTrafficShaper{}
}

func (ts *plainTrafficShaper) Start() {
}

func (ts *plainTrafficShaper) Stop() {
}

func (ts *plainTrafficShaper) AddTask(_ string, _ *peerTaskConductor) {
}

func (ts *plainTrafficShaper) RemoveTask(_ string) {
}

func (ts *plainTrafficShaper) Record(_ string, _ int) {
}

type taskEntry struct {
	ptc           *peerTaskConductor
	pieceSize     uint32
	usedBandwidth *atomic.Int64
	needBandwidth int64
	needUpdate    bool
}

type samplingTrafficShaper struct {
	*logger.SugaredLoggerOnWith
	sync.Mutex
	computePieceSize func(int64) uint32
	totalRateLimit   rate.Limit
	tasks            map[string]*taskEntry
	stopCh           chan struct{}
}

func NewSamplingTrafficShaper(totalRateLimit rate.Limit, computePieceSize func(int64) uint32) TrafficShaper {
	log := logger.With("component", "TrafficShaper")
	return &samplingTrafficShaper{
		SugaredLoggerOnWith: log,
		computePieceSize:    computePieceSize,
		totalRateLimit:      totalRateLimit,
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
	ts.Lock()
	defer ts.Unlock()
	// compute overall remaining length of all tasks
	for _, te := range ts.tasks {
		var needBandwidth int64
		oldLimit := int64(te.ptc.limiter.Limit())
		needBandwidth = te.usedBandwidth.Load()
		if !te.needUpdate {
			// if this task is added within 1 second, don't reduce its limit this time
			te.needUpdate = true
			if oldLimit > needBandwidth {
				needBandwidth = oldLimit
			}
		}
		if contentLength := te.ptc.contentLength.Load(); contentLength > 0 {
			remainingLength := contentLength - te.ptc.completedLength.Load()
			if remainingLength < needBandwidth {
				needBandwidth = remainingLength
			}
		}
		// delta bandwidth
		needBandwidth -= int64(te.pieceSize)
		if needBandwidth < 0 {
			needBandwidth = 0
		}
		te.needBandwidth = needBandwidth
		totalNeedBandwidth += needBandwidth
		totalLeastBandwidth += int64(te.pieceSize)
		te.usedBandwidth.Store(0)
	}

	// allocate delta bandwidth for tasks
	for _, te := range ts.tasks {
		var deltaLimit float64
		deltaLimit = (float64(ts.totalRateLimit) - float64(totalLeastBandwidth)) * (float64(te.needBandwidth) / float64(totalNeedBandwidth))
		// make sure new limit is not smaller than pieceSize
		if deltaLimit < 0 {
			deltaLimit = 0
		}
		te.ptc.limiter.SetLimit(rate.Limit(deltaLimit + float64(te.pieceSize)))
		ts.Debugf("period update limit, task %s, need bandwidth %d, delta rate limit %f", te.ptc.taskID, te.needBandwidth, deltaLimit)
	}
}

func (ts *samplingTrafficShaper) AddTask(taskID string, ptc *peerTaskConductor) {
	ts.Lock()
	defer ts.Unlock()
	nTasks := len(ts.tasks)
	if nTasks == 0 {
		nTasks++
	}
	limit := rate.Limit(float64(ts.totalRateLimit) / float64(nTasks))
	ptc.limiter.SetLimit(limit)
	pieceSize := ts.computePieceSize(ptc.contentLength.Load())
	ts.tasks[taskID] = &taskEntry{ptc: ptc, usedBandwidth: atomic.NewInt64(0), pieceSize: pieceSize}
	var totalNeedRateLimit rate.Limit
	for _, te := range ts.tasks {
		totalNeedRateLimit += te.ptc.limiter.Limit()
	}
	ratio := ts.totalRateLimit / totalNeedRateLimit
	// reduce all running tasks' bandwidth
	for _, te := range ts.tasks {
		newLimit := ratio * te.ptc.limiter.Limit()
		// make sure bandwidth is not smaller than pieceSize
		if float64(newLimit) < float64(te.pieceSize) {
			newLimit = rate.Limit(te.pieceSize)
		}
		te.ptc.limiter.SetLimit(newLimit)
		ts.Debugf("a task added, task %s rate limit updated to %f", te.ptc.taskID, newLimit)
	}
}

func (ts *samplingTrafficShaper) RemoveTask(taskID string) {
	ts.Lock()
	defer ts.Unlock()
	limit := ts.tasks[taskID].ptc.limiter.Limit()
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
	ts.Lock()
	ts.tasks[taskID].usedBandwidth.Add(int64(n))
	ts.Unlock()
}
