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
	"fmt"
	"time"

	"k8s.io/client-go/util/workqueue"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	scheduler2 "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type JobType int8

type Worker struct {
	scheduleQueue          workqueue.Interface
	updatePieceResultQueue chan *scheduler2.PieceResult
	sender                 ISender
	stopCh                 <-chan struct{}
	sendJob                func(*types.PeerTask)

	scheduler *scheduler.Scheduler
}

func CreateWorker(sche *scheduler.Scheduler, sender ISender, sendJod func(*types.PeerTask), stop <-chan struct{}) *Worker {
	return &Worker{
		scheduleQueue:          workqueue.New(),
		updatePieceResultQueue: make(chan *scheduler2.PieceResult, 100000),
		stopCh:                 stop,
		sender:                 sender,
		scheduler:              sche,
		sendJob:                sendJod,
	}
}

func (w *Worker) Start() {
	go w.doScheduleWorker()
	go w.doUpdatePieceResultWorker()
}

func (w *Worker) Stop() {
	if w == nil {
		return
	}
	w.scheduleQueue.ShutDown()
	close(w.updatePieceResultQueue)
}

func (w *Worker) ReceiveUpdatePieceResult(pr *scheduler2.PieceResult) {
	w.updatePieceResultQueue <- pr
}

func (w *Worker) doUpdatePieceResultWorker() {
	for {
		pr, ok := <-w.updatePieceResultQueue
		if !ok {
			return
		}
		peerTask, needSchedule, err := w.UpdatePieceResult(pr)
		if needSchedule {
			w.sendJob(peerTask)
		}

		if err != nil {
			logger.Errorf("[%s][%s]: update piece result failed %v", pr.TaskId, pr.SrcPid, err.Error())
		}
	}
}

func (w *Worker) UpdatePieceResult(pr *scheduler2.PieceResult) (peerTask *types.PeerTask, needSchedule bool, err error) {
	if pr == nil {
		return
	}

	if w.processErrorCode(pr) {
		return
	}

	ptMgr := mgr.GetPeerTaskManager()
	peerTask, _ = ptMgr.GetPeerTask(pr.SrcPid)
	if peerTask == nil {
		task, _ := mgr.GetTaskManager().GetTask(pr.TaskId)
		if task != nil {
			peerTask = ptMgr.AddFakePeerTask(pr.SrcPid, task)
		} else {
			err = fmt.Errorf("[%s][%s]: task not exited", pr.TaskId, pr.SrcPid)
			logger.Errorf(err.Error())
			return
		}
	}
	defer ptMgr.UpdatePeerTask(peerTask)
	var dstPeerTask *types.PeerTask
	if pr.DstPid == "" {
		if peerTask.GetParent() == nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			needSchedule = true
			ptMgr.RefreshDownloadMonitor(peerTask)
			return
		}
	} else {
		dstPeerTask, _ = ptMgr.GetPeerTask(pr.DstPid)
		if dstPeerTask == nil {
			dstPeerTask = ptMgr.AddFakePeerTask(pr.DstPid, peerTask.Task)
		}
	}

	if pr.PieceNum < 0 {
		if peerTask.GetParent() != nil {
			w.sendScheduleResult(peerTask)
		}
		return
	}

	peerTask.AddPieceStatus(pr)
	status := peerTask.GetNodeStatus()
	if peerTask.Success || status == types.PeerTaskStatusDone || peerTask.IsDown() {
		return
	}
	if dstPeerTask != nil && peerTask.GetParent() == nil {
		peerTask.SetNodeStatus(types.PeerTaskStatusAddParent, dstPeerTask)
		needSchedule = true
	} else if status == types.PeerTaskStatusHealth && w.scheduler.IsNodeBad(peerTask) {
		peerTask.SetNodeStatus(types.PeerTaskStatusBadNode)
		needSchedule = true
	} else if status == types.PeerTaskStatusHealth && w.scheduler.NeedAdjustParent(peerTask) {
		peerTask.SetNodeStatus(types.PeerTaskStatusNeedAdjustNode)
		needSchedule = true
	}

	ptMgr.RefreshDownloadMonitor(peerTask)

	return
}

func (w *Worker) ReceiveJob(peerTask *types.PeerTask) {
	logger.Debugf("doScheduleWorker begin add [%s]", peerTask.Pid)
	w.scheduleQueue.Add(peerTask)
}

func (w *Worker) sendJobLater(peerTask *types.PeerTask) {
	mgr.GetPeerTaskManager().RefreshDownloadMonitor(peerTask)
}

func (w *Worker) doScheduleWorker() {
	defer logger.Debugf("doScheduleWorker return")
	for {
		logger.Debugf("doScheduleWorker begin get")
		job, shutdown := w.scheduleQueue.Get()
		logger.Debugf("doScheduleWorker end get")
		if shutdown {
			return
		}
		peerTask, _ := job.(*types.PeerTask)
		w.doSchedule(peerTask)
		logger.Debugf("doScheduleWorker begin done [%s]", peerTask.Pid)
		w.scheduleQueue.Done(job)
		logger.Debugf("doScheduleWorker end done [%s]", peerTask.Pid)
	}
}

func (w *Worker) doSchedule(peerTask *types.PeerTask) {
	if peerTask == nil {
		return
	}

	startTm := time.Now()
	status := peerTask.GetNodeStatus()
	logger.Debugf("[%s][%s]: begin do schedule [%d]", peerTask.Task.TaskId, peerTask.Pid, status)
	defer func() {
		err := recover()
		if err != nil {
			logger.Errorf("[%s][%s]: do schedule panic: %v", peerTask.Task.TaskId, peerTask.Pid, err)
		}
		logger.Infof("[%s][%s]: end do schedule [%d] cost: %d", peerTask.Task.TaskId, peerTask.Pid, status, time.Now().Sub(startTm).Nanoseconds())
	}()

	switch status {
	case types.PeerTaskStatusAddParent:
		parent, _ := peerTask.GetJobData().(*types.PeerTask)
		if parent == nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusHealth)
			return
		}
		peerTask.AddParent(parent, 1)
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)
		return
	case types.PeerTaskStatusNeedParent:
		parent, _, err := w.scheduler.SchedulerParent(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule parent failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
		}
		// retry scheduler parent later when this is no parent
		if parent == nil || err != nil {
			w.sendJobLater(peerTask)
		} else {
			w.sendScheduleResult(peerTask)
			peerTask.SetNodeStatus(types.PeerTaskStatusHealth)
		}
		mgr.GetPeerTaskManager().RefreshDownloadMonitor(peerTask)
	case types.PeerTaskStatusNeedChildren:
		children, err := w.scheduler.SchedulerChildren(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule children failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			return
		}
		for i := range children {
			if children[i].GetParent() != nil {
				w.sendScheduleResult(children[i])
			} else {
				children[i].SetNodeStatus(types.PeerTaskStatusNeedParent)
				w.sendJob(children[i])
			}
		}
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusBadNode:
		adjustNodes, err := w.scheduler.SchedulerBadNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		for i := range adjustNodes {
			if adjustNodes[i].GetParent() != nil {
				w.sendScheduleResult(adjustNodes[i])
			} else {
				adjustNodes[i].SetNodeStatus(types.PeerTaskStatusNeedParent)
				w.sendJob(adjustNodes[i])
			}
		}
		if peerTask.GetParent() == nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			w.sendJobLater(peerTask)
		}

	case types.PeerTaskStatusNeedAdjustNode:
		_, _, err := w.scheduler.SchedulerAdjustParentNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		w.sendScheduleResult(peerTask)
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusNeedCheckNode:
		if w.scheduler.IsNodeBad(peerTask) && peerTask.GetSubTreeNodesNum() > 1 {
			adjustNodes, err := w.scheduler.SchedulerBadNode(peerTask)
			if err != nil {
				logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
				peerTask.SetNodeStatus(types.PeerTaskStatusBadNode)
				w.sendJobLater(peerTask)
				return
			}
			for i := range adjustNodes {
				if adjustNodes[i].GetParent() != nil {
					w.sendScheduleResult(adjustNodes[i])
				} else {
					adjustNodes[i].SetNodeStatus(types.PeerTaskStatusNeedParent)
					w.sendJob(adjustNodes[i])
				}
			}
			peerTask.SetNodeStatus(types.PeerTaskStatusHealth)
		} else if w.scheduler.NeedAdjustParent(peerTask) {
			_, _, err := w.scheduler.SchedulerAdjustParentNode(peerTask)
			if err != nil {
				logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
				return
			}
			w.sendScheduleResult(peerTask)
			peerTask.SetNodeStatus(types.PeerTaskStatusHealth)
		}

	case types.PeerTaskStatusDone:
		parent, err := w.scheduler.SchedulerDone(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		if parent != nil {
			parent.SetNodeStatus(types.PeerTaskStatusNeedChildren)
			w.sendJob(parent)
		}

	case types.PeerTaskStatusLeaveNode, types.PeerTaskStatusNodeGone:
		adjustNodes, err := w.scheduler.SchedulerLeaveNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		mgr.GetPeerTaskManager().DeletePeerTask(peerTask.Pid)
		logger.Debugf("[%s][%s]: PeerTaskStatusLeaveNode", peerTask.Task.TaskId, peerTask.Pid)
		for i := range adjustNodes {
			if adjustNodes[i].GetParent() != nil {
				w.sendScheduleResult(adjustNodes[i])
			} else {
				adjustNodes[i].SetNodeStatus(types.PeerTaskStatusNeedParent)
				w.sendJob(adjustNodes[i])
			}
		}

		// delete from manager
		mgr.GetPeerTaskManager().DeletePeerTask(peerTask.Pid)
		// delete from host
		peerTask.Host.DeletePeerTask(peerTask.Pid)
	}
	return
}

func (w *Worker) sendScheduleResult(peerTask *types.PeerTask) {
	if peerTask == nil {
		return
	}
	parent := "nil"
	if peerTask.GetParent() != nil && peerTask.GetParent().DstPeerTask != nil {
		parent = peerTask.GetParent().DstPeerTask.Pid
	}
	logger.Infof("[%s][%s]: sendScheduleResult parent[%s] active time[%d] deep [%d]", peerTask.Task.TaskId, peerTask.Pid, parent, time.Now().UnixNano()-peerTask.GetStartTime(), peerTask.GetDeep())
	w.sender.Send(peerTask)
	return
}

func (w *Worker) processErrorCode(pr *scheduler2.PieceResult) (stop bool) {
	code := pr.Code

	switch code {
	case dfcodes.Success:
		return
	case dfcodes.PeerTaskNotFound:
		peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
		if peerTask != nil {
			parent := peerTask.GetParent()
			if parent != nil && parent.DstPeerTask != nil {
				pNode := parent.DstPeerTask
				pNode.SetNodeStatus(types.PeerTaskStatusLeaveNode)
				w.sendJob(pNode)
			}
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			w.sendJob(peerTask)
		}
		return true
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
		if peerTask != nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			w.sendJob(peerTask)
		}
		return true
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail:
		peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
		if peerTask != nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			w.sendJob(peerTask)
			task := peerTask.Task
			if task != nil {
				mgr.GetCDNManager().TriggerTask(task)
			}
		}
		return true
	case dfcodes.UnknownError:
		return true
	}

	return
}
