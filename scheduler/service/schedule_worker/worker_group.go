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

package schedule_worker

import (
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	scheduler2 "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
	"hash/crc32"
	"k8s.io/client-go/util/workqueue"
)

type IWorker interface {
	Start()
	Stop()
	ReceiveJob(job *types.PeerTask)
	ReceiveUpdatePieceResult(pr *scheduler2.PieceResult)
}

type WorkerGroup struct {
	workerNum  int
	chanSize   int
	workerList []*Worker
	stopCh     chan struct{}
	sender     ISender

	triggerLoadQueue workqueue.Interface

	scheduler *scheduler.Scheduler
}

func CreateWorkerGroup(scheduler *scheduler.Scheduler) *WorkerGroup {
	workerNum := config.GetConfig().Worker.WorkerNum
	chanSize := config.GetConfig().Worker.WorkerJobPoolSize
	return &WorkerGroup{
		workerNum:        workerNum,
		chanSize:         chanSize,
		sender:           CreateSender(),
		scheduler:        scheduler,
		triggerLoadQueue: workqueue.New(),
	}
}

func (wg *WorkerGroup) Start() {
	wg.stopCh = make(chan struct{})

	mgr.GetPeerTaskManager().SetDownloadingMonitorCallBack(func(pt *types.PeerTask) {
		if pt.GetNodeStatus() != types.PeerTaskStatusHealth {
		} else if pt.GetNodeStatus() != types.PeerTaskStatusDone{
			return
		} else if pt.Success || pt.Host.Type == types.HostTypeCdn {
			return
		} else if pt.GetParent() == nil  {
			pt.SetNodeStatus(types.PeerTaskStatusNeedParent)
		} else {
			pt.SetNodeStatus(types.PeerTaskStatusNeedCheckNode)
		}
		wg.ReceiveJob(pt)
	})

	for i := 0; i < wg.workerNum; i++ {
		w := CreateWorker(wg.scheduler, wg.sender, wg.ReceiveJob, wg.stopCh)
		w.Start()
		wg.workerList = append(wg.workerList, w)
	}
	wg.sender.Start()
	logger.Infof("start scheduler worker number:%d", wg.workerNum)
}

func (wg *WorkerGroup) Stop() {
	close(wg.stopCh)
	wg.sender.Stop()
	wg.triggerLoadQueue.ShutDown()
	logger.Infof("stop scheduler worker")
}

func (wg *WorkerGroup) ReceiveJob(job *types.PeerTask) {
	if job == nil {
		return
	}
	choiceWorkerId := crc32.ChecksumIEEE([]byte(job.Task.TaskId)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerId].ReceiveJob(job)
}

func (wg *WorkerGroup) ReceiveUpdatePieceResult(pr *scheduler2.PieceResult) {
	if pr == nil {
		return
	}
	choiceWorkerId := crc32.ChecksumIEEE([]byte(pr.SrcPid)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerId].ReceiveUpdatePieceResult(pr)
}
