package schedule_worker

import (
	dferror "github.com/dragonflyoss/Dragonfly2/pkg/error"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"k8s.io/client-go/util/workqueue"
)

type Worker struct {
	queue workqueue.Interface
	sender  ISender
	stopCh  <-chan struct{}

	scheduler *scheduler.Scheduler
}

func CreateWorker(sche *scheduler.Scheduler, sender ISender, stop <-chan struct{}) *Worker {
	return &Worker{
		queue:   workqueue.New(),
		stopCh:    stop,
		sender:    sender,
		scheduler: sche,
	}
}

func (w *Worker) Start() {
	go w.doWorker()
}

func (w *Worker) ReceiveJob(job *types.PeerTask) {
	w.queue.Add(job)
}

func (w *Worker) doWorker() {
	for {
		job, shutdown := w.queue.Get()
		if shutdown {
			return
		}
		pt, _ := job.(*types.PeerTask)
		if pt != nil {
			pkg := w.doSchedule(pt)
			if pkg != nil {
				w.sendScheduleResult(pt.Pid, pkg)
			}
		}
		w.queue.Done(job)
	}
}

func (w *Worker) doSchedule(peerTask *types.PeerTask) (pkg *scheduler2.PiecePackage) {
	var pieceTaskList []*types.PieceTask
	var waitingPieceNumList []int32

	logger.Debugf("[%s][%s]: begin do schedule", peerTask.Task.TaskId, peerTask.Pid)
	defer func() {
		logger.Debugf("[%s][%s]: end do schedule", peerTask.Task.TaskId, peerTask.Pid)
		if pkg == nil {
			logger.Debugf("[%s][%s]: end do schedule wait %v", peerTask.Task.TaskId, peerTask.Pid, waitingPieceNumList)
		} else if pkg.Done {
			logger.Debugf("[%s][%s]: end do schedule done ", peerTask.Task.TaskId, peerTask.Pid)
		} else {
			logger.Debugf("[%s][%s]: end do schedule pkg-%v", peerTask.Task.TaskId, peerTask.Pid, pkg.PieceTasks)
		}
	} ()
	pieceTaskList, waitingPieceNumList, err := w.scheduler.Scheduler(peerTask)
	if err != nil {
		switch err {
		case dferror.SchedulerWaitPiece:
			for _, pieceNum := range waitingPieceNumList {
				piece := peerTask.Task.GetOrCreatePiece(pieceNum)
				peerTask.ScheduleTrigger = w
				piece.AddWaitPeerTask(peerTask)
			}
			return nil
		case dferror.SchedulerFinished:
			pkg = new(scheduler2.PiecePackage)
			pkg.TaskId = peerTask.Task.TaskId
			pkg.Pid = peerTask.Pid
			pkg.Done = true
			pkg.ContentLength = peerTask.Task.ContentLength
		}
		return
	}
	// assemble result
	pkg = new(scheduler2.PiecePackage)
	pkg.TaskId = peerTask.Task.TaskId
	pkg.Pid = peerTask.Pid
	for _, p := range pieceTaskList {
		pkg.PieceTasks = append(pkg.PieceTasks, &scheduler2.PiecePackage_PieceTask{
			PieceNum:    p.Piece.PieceNum,
			PieceRange:  p.Piece.PieceRange,
			PieceMd5:    p.Piece.PieceMd5,
			SrcPid:      p.SrcPid,
			DstPid:      p.DstPid,
			DstAddr:     p.DstAddr,
			PieceOffset: p.Piece.PieceOffset,
			PieceStyle:  p.Piece.PieceStyle,
		})
	}
	return
}

func (w *Worker) sendScheduleResult(pid string, pkg *scheduler2.PiecePackage) {
	logger.Debugf("[%s][%s]: sendScheduleResult", pkg.TaskId, pid)
	w.sender.Send(pid, pkg)
	return
}
