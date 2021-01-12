package schedule_worker

import (
	"fmt"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
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
	ptMgr := mgr.GetPeerTaskManager()
	peerTask, _ = ptMgr.GetPeerTask(pr.SrcPid)
	if peerTask == nil {
		err = fmt.Errorf("[%s][%s]: peer task not exited", pr.TaskId, pr.SrcPid)
		logger.Errorf(err.Error())
		return
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
		}
	}

	if pr.PieceNum < 0 {
		if peerTask.GetParent() != nil {
			w.sendScheduleResult(peerTask)
		}
		return
	}

	peerTask.AddPieceStatus(&types.PieceStatus{
		PieceNum: pr.PieceNum,
		SrcPid:   pr.SrcPid,
		DstPid:   pr.DstPid,
		Success:  pr.Success,
		Code:     pr.Code,
		Cost:     uint32(pr.EndTime - pr.BeginTime),
	})
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

	logger.Debugf("[%s][%s]: begin do schedule", peerTask.Task.TaskId, peerTask.Pid)
	defer func() {
		logger.Debugf("[%s][%s]: end do schedule", peerTask.Task.TaskId, peerTask.Pid)
	}()

	switch peerTask.GetNodeStatus() {
	case types.PeerTaskStatusNeedParent:
		_, _, err := w.scheduler.SchedulerParent(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule parent failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			return
		}
		w.sendScheduleResult(peerTask)
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusNeedChildren:
		children, err := w.scheduler.SchedulerChildren(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule children failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			return
		}
		for _, child := range children {
			w.sendScheduleResult(child)
		}
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusBadNode:
		adjustNodes, err := w.scheduler.SchedulerBadNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			return
		}
		for _, node := range adjustNodes {
			w.sendScheduleResult(node)
		}
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusNeedAdjustNode:
		_, _, err := w.scheduler.SchedulerAdjustParentNode(peerTask)
		if err != nil {
			logger.Debugf("[%s][%s]: schedule adjust node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
			return
		}
		w.sendScheduleResult(peerTask)
		peerTask.SetNodeStatus(types.PeerTaskStatusHealth)

	case types.PeerTaskStatusNeedCheckNode:
		if w.scheduler.IsNodeBad(peerTask) {
			adjustNodes, err := w.scheduler.SchedulerBadNode(peerTask)
			if err != nil {
				logger.Debugf("[%s][%s]: schedule bad node failed: %v", peerTask.Task.TaskId, peerTask.Pid, err)
				return
			}
			for _, node := range adjustNodes {
				w.sendScheduleResult(node)
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
			return
		}
		if parent != nil {
			parent.SetNodeStatus(types.PeerTaskStatusNeedChildren)
			w.sendJob(parent)
		}
	}
	return
}

func (w *Worker) sendScheduleResult(peerTask *types.PeerTask) {
	logger.Debugf("[%s][%s]: sendScheduleResult", peerTask.Task.TaskId, peerTask.Pid)
	w.sender.Send(peerTask)
	return
}
