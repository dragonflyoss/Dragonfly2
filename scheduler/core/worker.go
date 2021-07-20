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

package core

import (
	"hash/crc32"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type worker interface {
	start(*state)
	stop()
	send(event) bool
}

type workerGroup struct {
	workerNum  int
	workerList []*baseWorker
	stopCh     chan struct{}
}

var _ worker = (*workerGroup)(nil)

func newEventLoopGroup(workerNum int) worker {
	return &workerGroup{
		workerNum:  workerNum,
		workerList: make([]*baseWorker, 0, workerNum),
		stopCh:     make(chan struct{}),
	}
}

func (wg *workerGroup) start(s *state) {
	for i := 0; i < wg.workerNum; i++ {
		w := newWorker()
		go w.start(s)
		wg.workerList = append(wg.workerList, w)
	}

	logger.Infof("start scheduler worker number:%d", wg.workerNum)
}

func (wg *workerGroup) send(e event) bool {
	choiceWorkerID := crc32.ChecksumIEEE([]byte(e.hashKey())) % uint32(wg.workerNum)
	return wg.workerList[choiceWorkerID].send(e)
}

func (wg *workerGroup) stop() {
	close(wg.stopCh)
	for _, worker := range wg.workerList {
		worker.stop()
	}
}

type baseWorker struct {
	events chan event
	done   chan struct{}
}

var _ worker = (*baseWorker)(nil)

func newWorker() *baseWorker {
	return &baseWorker{
		events: make(chan event),
		done:   make(chan struct{}),
	}
}

func (w *baseWorker) start(s *state) {
	for {
		select {
		case e := <-w.events:
			e.apply(s)
		case <-w.done:
			return
		}
	}
}

func (w *baseWorker) stop() {
	close(w.done)
}

func (w *baseWorker) send(e event) bool {
	select {
	case w.events <- e:
		return true
	case <-w.done:
		return false
	}
}
