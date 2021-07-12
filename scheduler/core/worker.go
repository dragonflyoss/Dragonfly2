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
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/panjf2000/ants/v2"
)

type Worker struct {
	pool *ants.Pool
	// cdn mgr
	cdnManager daemon.CDNMgr
	// task mgr
	taskManager daemon.TaskMgr
	// host mgr
	hostManager daemon.HostMgr
	// Peer mgr
	peerManager daemon.PeerMgr

	sched *Scheduler
}

func newWorker(cfg *config.SchedulerConfig, sched *Scheduler, cdnManager daemon.CDNMgr, taskManager daemon.TaskMgr, hostManager daemon.HostMgr,
	peerManager daemon.PeerMgr) (*Worker,
	error) {
	pool, err := ants.NewPool(cfg.WorkerNum)
	if err != nil {
		return nil, err
	}
	return &Worker{
		pool:        pool,
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		sched:       sched,
	}, nil
}

func (worker *Worker) Submit(task func()) error {
	return worker.pool.Submit(task)
}

func NewLeaveTask(worker *Worker, target *scheduler.PeerTarget) func() {
	return func() {
		peerNode, _ := worker.peerManager.Get(target.PeerId)
		adjustNodes, err := worker.Scheduler.ScheduleLeaveNode(peerNode)
		if err != nil {

		}
		for idx := range adjustNodes {
			if adjustNodes[idx].Parent != nil {

			}
		}

	}
}

func NewReportPeerResultTask(worker *Worker, result *scheduler.PeerResult) func() {

	return nil
}

func NewReportPieceResultTask(worker *Worker, pr *scheduler.PieceResult) func() {
	return func() {
		if pr == nil || worker.processErrorCode(pr) {
			return
		}
		peer, ok := worker.peerManager.Get(pr.SrcPid)
		if !ok {

		}
	}
}

func (worker *Worker) processErrorCode(pr *scheduler.PieceResult) (stop bool) {
	code := pr.Code
	switch code {
	case dfcodes.Success:
		return
	case dfcodes.PeerTaskNotFound:
		peerTask, _ := worker.peerManager.Get(pr.SrcPid)
		if peerTask != nil {
			parent := peerTask.Parent
			if parent != nil {
				pNode := parent.DstPeerTask
				pNode.SetNodeStatus(types.PeerTaskStatusLeaveNode)
				w.sendJob(pNode)
			}
			peerTask.SetNodeStatus(types.PeerStatusNeedParent)
			w.sendJob(peerTask)
		}
		return true
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		peerTask, _ := w.schedulerService.TaskManager.PeerTask.Get(pr.SrcPid)
		if peerTask != nil {
			peerTask.SetNodeStatus(types.PeerStatusNeedParent)
			worker.sendJob(peerTask)
		}
		return true
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail:
		peerTask, _ := w.schedulerService.TaskManager.PeerTask.Get(pr.SrcPid)
		if peerTask != nil {
			peerTask.SetNodeStatus(types.PeerStatusNeedParent)
			w.sendJob(peerTask)
			task := peerTask.Task
			if task != nil {
				if task.CDNError != nil {
					go safe.Call(func() { peerTask.SendError(task.CDNError) })
				} else {
					worker.CDNManager.TriggerTask(task, w.schedulerService.TaskManager.PeerTask.CDNCallback)
				}
			}
		}
		return true
	case dfcodes.UnknownError:
		return true
	}

	return
}
