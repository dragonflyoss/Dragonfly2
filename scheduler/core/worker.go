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
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/panjf2000/ants/v2"
)

type Sender struct {
	pool             *ants.Pool
	schedulerService *SchedulerService
}

type Worker struct {
	pool             *ants.Pool
	sender           *Sender
	schedulerService *SchedulerService
}

func NewWorker(cfg *config.SchedulerConfig) (*Worker, error) {
	pool, err := ants.NewPool(cfg.WorkerNum)
	if err != nil {
		return nil, err
	}
	return &Worker{
		pool:             pool,
		schedulerService: nil,
	}, nil
}

func (worker *Worker) Submit(task func()) error {
	aa := func() {

	}
	return worker.pool.Submit(task)
}

func NewLeaveTask(service *SchedulerService, target *scheduler.PeerTarget) func() {
	return func() {
		children := target.Children
		service.ScheduleParent()
		service.DeletePeerTask(peer.PeerID)

	}
}

func NewReportPeerResultTask(service *SchedulerService, result *scheduler.PeerResult) func() {
	return nil
}

func NewReportPieceResultTask(service *SchedulerService, result *scheduler.PieceResult) func() {
	return nil
}
