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

package worker

import (
	"hash/crc32"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	scheduler2 "d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/types"
	"k8s.io/client-go/util/workqueue"
)

type Worker interface {
	Serve()
	Stop()
	ReceivePeerTask(pt *types.PeerTask)
	ReceivePieceResult(pr *scheduler2.PieceResult)
}

type Group struct {
	workerNum  int
	chanSize   int
	workerList []Worker
	stopCh     chan struct{}
	sender     Sender

	triggerLoadQueue workqueue.Interface

	schedulerService *service.SchedulerService
}

var _ Worker = (*Group)(nil)

func NewGroup(cfg *config.Config, schedulerService *service.SchedulerService) *Group {
	return &Group{
		workerNum:        cfg.Worker.WorkerNum,
		chanSize:         cfg.Worker.WorkerJobPoolSize,
		sender:           NewSender(cfg.Worker, schedulerService),
		schedulerService: schedulerService,
		triggerLoadQueue: workqueue.New(),
	}
}

func (wg *Group) Serve() {
	wg.stopCh = make(chan struct{})

	wg.schedulerService.TaskManager.PeerTask.SetDownloadingMonitorCallBack(func(pt *types.PeerTask) {
		status := pt.GetNodeStatus()
		if status != types.PeerTaskStatusHealth {
			//} else if pt.GetNodeStatus() != types.PeerTaskStatusDone{
			//	return
		} else if pt.Success || pt.Host.Type == types.HostTypeCdn {
			return
		} else if pt.GetParent() == nil {
			pt.SetNodeStatus(types.PeerTaskStatusNeedParent)
		} else {
			pt.SetNodeStatus(types.PeerTaskStatusNeedCheckNode)
		}
		wg.ReceivePeerTask(pt)
	})

	for i := 0; i < wg.workerNum; i++ {
		w := NewWorker(wg.schedulerService, wg.sender, wg.ReceivePeerTask, wg.stopCh)
		w.Serve()
		wg.workerList = append(wg.workerList, w)
	}

	wg.sender.Serve()

	logger.Infof("start scheduler worker number:%d", wg.workerNum)
}

func (wg *Group) Stop() {
	close(wg.stopCh)
	wg.sender.Stop()
	wg.triggerLoadQueue.ShutDown()
	logger.Infof("stop scheduler worker")
}

func (wg *Group) ReceivePeerTask(pt *types.PeerTask) {
	if pt == nil {
		return
	}
	choiceWorkerID := crc32.ChecksumIEEE([]byte(pt.Task.TaskID)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerID].ReceivePeerTask(pt)
}

func (wg *Group) ReceivePieceResult(pr *scheduler2.PieceResult) {
	if pr == nil {
		return
	}
	choiceWorkerID := crc32.ChecksumIEEE([]byte(pr.SrcPid)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerID].ReceivePieceResult(pr)
}
