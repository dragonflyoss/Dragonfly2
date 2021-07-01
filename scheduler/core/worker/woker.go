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
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type JobType int8

// 实现 IWorker 接口
type Worker struct {
	peerChan        chan *types.PeerNode
	pieceResultChan chan *scheduler.PieceResult
	sender          ISender
	stopCh          <-chan struct{}
	sendJob         func(*types.PeerNode)

	schedulerService *core.SchedulerService
}

var _ IWorker = (*Worker)(nil)

func NewWorker(schedulerService *core.SchedulerService, sender ISender, sendJod func(*types.PeerNode), stop <-chan struct{}) *Worker {
	return &Worker{
		peerChan:         make(chan *types.PeerNode),
		pieceResultChan:  make(chan *scheduler.PieceResult),
		stopCh:           stop,
		sender:           sender,
		schedulerService: schedulerService,
		sendJob:          sendJod,
	}
}

func (w *Worker) Serve() {
	go safe.Call(w.startScheduleWorker)
	go safe.Call(w.startPieceResultWorker)
}

func (w *Worker) Stop() {
	close(w.peerChan)
	close(w.pieceResultChan)
}

func (w *Worker) ReceiveUpdatePieceResult(pieceResult *scheduler.PieceResult) {
	w.pieceResultChan <- pieceResult
}

func (w *Worker) ReceiveJob(peer *types.PeerNode) {
	logger.Debugf("doScheduleWorker begin add [%s]", peer.GetPeerID())
	w.peerChan <- peer
}

func (w *Worker) startPieceResultWorker() {
	for {
		pieceResult, ok := <-w.pieceResultChan
		if !ok {
			return
		}
		peerTask, needSchedule, err := w.updatePieceResult(pieceResult)
		if needSchedule {
			w.sendJob(peerTask)
		}

		if err != nil {
			logger.Errorf("[%s][%s]: update piece result failed %v", pieceResult.TaskId, pieceResult.SrcPid, err.Error())
		}
	}
}

func (w *Worker) updatePieceResult(pieceResult *scheduler.PieceResult) (peer *types.PeerNode, needSchedule bool, err error) {
	if pieceResult == nil {
		return
	}

	if w.processErrorCode(pieceResult) {
		return
	}

	peerNode, _ := w.schedulerService.PeerManager.Get(pieceResult.SrcPid)
	if peerNode == nil {
		task, _ := w.schedulerService.TaskManager.Load(pieceResult.TaskId)
		if task != nil {
			peerTask = w.schedulerService.PeerManager.AddFake(pieceResult.SrcPid, task)
		} else {
			err = fmt.Errorf("[%s][%s]: task not exited", pr.TaskId, pr.SrcPid)
			logger.Errorf(err.Error())
			return
		}
	}
	defer w.schedulerService.PeerManager.Update(peerNode)

	var dstPeerTask *types.PeerNode
	if pieceResult.DstPid == "" {
		if peerNode.GetParent() == nil {
			peerNode.Status = types.PeerStatusNeedParent
			needSchedule = true
			w.schedulerService.PeerManager.RefreshDownloadMonitor(peerNode)
			return
		}
	} else {
		dstPeerNode, _ := &w.schedulerService.PeerManager.Get(pieceResult.DstPid)
		if dstPeerNode == nil {
			dstPeerTask = pt.AddFake(pr.DstPid, peerTask.Task)
		}
	}

	if pieceResult.PieceNum < 0 {
		if peerTask.GetParent() != nil {
			w.sendScheduleResult(peerTask)
		}
		return
	}

	peerTask.AddPieceStatus(pr)
	status := peerTask.GetNodeStatus()
	if peerTask.Success || status == types.PeerStatusDone || peerTask.IsDown() {
		return
	}
	if dstPeerTask != nil && peerTask.GetParent() == nil {
		peerTask.SetNodeStatus(types.PeerStatusAddParent, dstPeerTask)
		needSchedule = true
	} else if status == types.PeerStatusHealth && w.schedulerService.Scheduler.IsNodeBad(peerTask) {
		peerTask.SetNodeStatus(types.PeerStatusBadNode)
		needSchedule = true
	} else if status == types.PeerStatusHealth && w.schedulerService.Scheduler.NeedAdjustParent(peerTask) {
		peerTask.SetNodeStatus(types.PeerStatusNeedAdjustNode)
		needSchedule = true
	}

	pt.RefreshDownloadMonitor(peerTask)

	return
}

func (w *Worker) sendJobLater(peerTask *types.PeerTask) {
	w.schedulerService.TaskManager.PeerTask.RefreshDownloadMonitor(peerTask)
}

func (w *Worker) startScheduleWorker() {
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

func (w *Worker) doSchedule(peerTask *types.PeerNode) {
	if peerTask == nil {
		return
	}

	startTm := time.Now()
	status := peerTask.GetNodeStatus()
	logger.Debugf("[%s][%s]: begin do schedule [%d]", peerTask.Task.TaskID, peerTask.Pid, status)
	defer func() {
		err := recover()
		if err != nil {
			logger.Errorf("[%s][%s]: do schedule panic: %v", peerTask.Task.TaskID, peerTask.Pid, err)
		}
		logger.Infof("[%s][%s]: end do schedule [%d] cost: %d", peerTask.Task.TaskID, peerTask.Pid, status, time.Now().Sub(startTm).Nanoseconds())
	}()

	switch status {
	case types.PeerStatusAddParent:
		parent, _ := peerTask.GetJobData().(*types.PeerTask)
		if parent == nil {
			peerTask.SetNodeStatus(types.PeerStatusHealth)
			return
		}
		peerTask.SetParent(parent, 1)
		peerTask.SetNodeStatus(types.PeerStatusHealth)
		return
	case types.PeerStatusNeedParent:
		parent, _, err := w.schedulerService.Scheduler.ScheduleParent(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule parent failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
		}
		// retry scheduler parent later when this is no parent
		if parent == nil || err != nil {
			w.sendJobLater(peerTask)
		} else {
			w.sendScheduleResult(peerTask)
			peerTask.SetNodeStatus(types.PeerStatusHealth)
		}
		w.schedulerService.TaskManager.PeerTask.RefreshDownloadMonitor(peerTask)
	case types.PeerStatusNeedChildren:
		children, err := w.schedulerService.Scheduler.ScheduleChildren(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule children failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
			return
		}
		for i := range children {
			if children[i].GetParent() != nil {
				w.sendScheduleResult(children[i])
			} else {
				children[i].SetNodeStatus(types.PeerStatusNeedParent)
				w.sendJob(children[i])
			}
		}
		peerTask.SetNodeStatus(types.PeerStatusHealth)

	case types.PeerStatusBadNode:
		adjustNodes, err := w.schedulerService.Scheduler.ScheduleBadNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		for i := range adjustNodes {
			if adjustNodes[i].GetParent() != nil {
				w.sendScheduleResult(adjustNodes[i])
			} else {
				adjustNodes[i].SetNodeStatus(types.PeerStatusNeedParent)
				w.sendJob(adjustNodes[i])
			}
		}
		if peerTask.GetParent() == nil {
			peerTask.SetNodeStatus(types.PeerStatusNeedParent)
			w.sendJobLater(peerTask)
		}

	case types.PeerStatusNeedAdjustNode:
		_, _, err := w.schedulerService.Scheduler.ScheduleAdjustParentNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		w.sendScheduleResult(peerTask)
		peerTask.SetNodeStatus(types.PeerStatusHealth)

	case types.PeerStatusNeedCheckNode:
		if w.schedulerService.Scheduler.IsNodeBad(peerTask) && peerTask.GetSubTreeNodesNum() > 1 {
			adjustNodes, err := w.schedulerService.Scheduler.ScheduleBadNode(peerTask)
			if err != nil {
				logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
				peerTask.SetNodeStatus(types.PeerStatusBadNode)
				w.sendJobLater(peerTask)
				return
			}
			for i := range adjustNodes {
				if adjustNodes[i].GetParent() != nil {
					w.sendScheduleResult(adjustNodes[i])
				} else {
					adjustNodes[i].SetNodeStatus(types.PeerStatusNeedParent)
					w.sendJob(adjustNodes[i])
				}
			}
			peerTask.SetNodeStatus(types.PeerStatusHealth)
		} else if w.schedulerService.Scheduler.NeedAdjustParent(peerTask) {
			_, _, err := w.schedulerService.Scheduler.ScheduleAdjustParentNode(peerTask)
			if err != nil {
				logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
				return
			}
			w.sendScheduleResult(peerTask)
			peerTask.SetNodeStatus(types.PeerStatusHealth)
		}

	case types.PeerStatusDone:
		parent, err := w.schedulerService.Scheduler.ScheduleDone(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		if parent != nil {
			parent.SetNodeStatus(types.PeerStatusNeedChildren)
			w.sendJob(parent)
		}

	case types.PeerStatusLeaveNode, types.PeerStatusNodeGone:
		adjustNodes, err := w.schedulerService.Scheduler.ScheduleLeaveNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskID, peerTask.Pid, err)
			w.sendJobLater(peerTask)
			return
		}
		w.schedulerService.TaskManager.PeerTask.Delete(peerTask.Pid)
		logger.Debugf("[%s][%s]: PeerStatusLeaveNode", peerTask.Task.TaskID, peerTask.Pid)
		for i := range adjustNodes {
			if adjustNodes[i].GetParent() != nil {
				w.sendScheduleResult(adjustNodes[i])
			} else {
				adjustNodes[i].SetNodeStatus(types.PeerStatusNeedParent)
				w.sendJob(adjustNodes[i])
			}
		}

		// delete from manager
		w.schedulerService.TaskManager.PeerTask.Delete(peerTask.Pid)
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
	logger.Infof("[%s][%s]: sendScheduleResult parent[%s] active time[%d] deep [%d]", peerTask.Task.TaskID, peerTask.Pid, parent, time.Now().UnixNano()-peerTask.GetStartTime(), peerTask.GetDeep())
	w.sender.Send(peerTask)
	return
}

func (w *Worker) processErrorCode(pr *scheduler.PieceResult) (stop bool) {
	code := pr.Code

	switch code {
	case dfcodes.Success:
		return
	case dfcodes.PeerTaskNotFound:
		peerTask, _ := w.schedulerService.TaskManager.PeerTask.Get(pr.SrcPid)
		if peerTask != nil {
			parent := peerTask.GetParent()
			if parent != nil && parent.DstPeerTask != nil {
				pNode := parent.DstPeerTask
				pNode.SetNodeStatus(types.PeerStatusLeaveNode)
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
			w.sendJob(peerTask)
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
					w.schedulerService.CDNManager.TriggerTask(task, w.schedulerService.TaskManager.PeerTask.CDNCallback)
				}
			}
		}
		return true
	case dfcodes.UnknownError:
		return true
	}

	return
}
