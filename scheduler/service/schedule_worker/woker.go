package schedule_worker

import (
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	scheduler2 "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
	"fmt"
	"k8s.io/client-go/util/workqueue"
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
			w.ReceiveJob(peerTask)
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
	if pr.DstPid == "" {
		if peerTask.GetParent() == nil {
			peerTask.SetNodeStatus(types.PeerTaskStatusNeedParent)
			needSchedule = true
			return
		}
	} else {
		dstPeerTask, _ := ptMgr.GetPeerTask(pr.DstPid)
		if dstPeerTask == nil {
			ptMgr.AddFakePeerTask(pr.DstPid, peerTask.Task)
			peerTask.AddParent(dstPeerTask, 1)
		} else {
			if peerTask.GetParent() == nil {
				peerTask.AddParent(dstPeerTask, 1)
			}
		}
	}

	if pr.PieceNum < 0 {
		if peerTask.GetParent() != nil {
			w.sendScheduleResult(peerTask)
		}
		return
	}

	peerTask.AddPieceStatus(pr)
	if w.scheduler.IsNodeBad(peerTask) {
		peerTask.SetNodeStatus(types.PeerTaskStatusBadNode)
		needSchedule = true
	} else if w.scheduler.NeedAdjustParent(peerTask) {
		peerTask.SetNodeStatus(types.PeerTaskStatusNeedAdjustNode)
		needSchedule = true
	}

	mgr.GetPeerTaskManager().RefreshDownloadMonitor(peerTask)

	return
}

func (w *Worker) ReceiveJob(peerTask *types.PeerTask) {
	w.scheduleQueue.Add(peerTask)
}

func (w *Worker) sendJobLater(peerTask *types.PeerTask) {
	mgr.GetPeerTaskManager().RefreshDownloadMonitor(peerTask)
}

func (w *Worker) doScheduleWorker() {
	for {
		job, shutdown := w.scheduleQueue.Get()
		if shutdown {
			return
		}
		peerTask, _ := job.(*types.PeerTask)
		w.doSchedule(peerTask)
		w.scheduleQueue.Done(job)
	}
}

func (w *Worker) doSchedule(peerTask *types.PeerTask) {
	if peerTask == nil {
		return
	}

	logger.Debugf("[%s][%s]: begin do schedule [%d]", peerTask.Task.TaskId, peerTask.Pid, peerTask.GetNodeStatus())
	defer func() {
		logger.Debugf("[%s][%s]: end do schedule [%d]", peerTask.Task.TaskId, peerTask.Pid, peerTask.GetNodeStatus())
	}()

	switch peerTask.GetNodeStatus() {
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

	case types.PeerTaskStatusLeaveNode:
		adjustNodes, err := w.scheduler.SchedulerLeaveNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
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
	}
	return
}

func (w *Worker) sendScheduleResult(peerTask *types.PeerTask) {
	logger.Debugf("[%s][%s]: sendScheduleResult", peerTask.Task.TaskId, peerTask.Pid)
	w.sender.Send(peerTask)
	return
}

func (w *Worker) processErrorCode(pr *scheduler2.PieceResult) (stop bool) {
	code := pr.Code

	switch code {
	case dfcodes.Success:
		return
	}

	return
}


