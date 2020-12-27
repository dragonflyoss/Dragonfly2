package schedule_worker

import (
	"fmt"
	dferror "github.com/dragonflyoss/Dragonfly2/pkg/error"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"sync"
)

type Worker struct {
	jobChan chan *scheduler2.PieceResult
	jobMap  *sync.Map
	sender  ISender
	stopCh  <-chan struct{}

	scheduler *scheduler.Scheduler
}

func CreateWorker(sche *scheduler.Scheduler, sender ISender, chanSize int, stop <-chan struct{}) *Worker {
	return &Worker{
		jobChan:   make(chan *scheduler2.PieceResult, chanSize),
		jobMap:    new(sync.Map),
		stopCh:    stop,
		sender:    sender,
		scheduler: sche,
	}
}

func (w *Worker) Start() {
	go w.doWorker()
}

func (w *Worker) ReceiveJob(job *scheduler2.PieceResult) {
	_, loaded := w.jobMap.LoadOrStore(job.SrcPid, job)
	if loaded {
		if job.ErrorCode != base.Code_SCHEDULER_RETRY_ERROR {
			w.jobMap.Store(job.SrcPid, job)
		}
	}
	select {
	case w.jobChan <- job:
	default:
		logger.Warnf("Worker job channel is full")
	}
}

func (w *Worker) doWorker() {
	for {
		select {
		case pr := <-w.jobChan:
			v, ok := w.jobMap.Load(pr.SrcPid)
			if ok {
				pieceResult, ok := v.(*scheduler2.PieceResult)
				if ok {
					w.updatePieceResult(pieceResult)
					pkg := w.doSchedule(pieceResult)
					if pkg != nil {
						w.sendScheduleResult(pr.SrcPid, pkg)
					}
				}
			}
			w.jobMap.Delete(pr.SrcPid)
		case <-w.stopCh:
			return
		}
	}
}

func (w *Worker) updatePieceResult(pr *scheduler2.PieceResult) (err error) {
	if pr.ErrorCode == base.Code_SCHEDULER_RETRY_ERROR || pr.PieceNum<0 {
		return
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
	if peerTask == nil {
		err = fmt.Errorf("peer task not exited : %s", pr.SrcPid)
		return
	}
	peerTask.AddPieceStatus(&types.PieceStatus{
		PieceNum:  pr.PieceNum,
		SrcPid:    pr.SrcPid,
		DstPid:    pr.DstPid,
		Success:   pr.Success,
		ErrorCode: pr.ErrorCode,
		Cost:      pr.Cost,
	})
	peerTask.DeleteDownloadingPiece(pr.PieceNum)
	dstPeerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.DstPid)
	if dstPeerTask != nil {
		dstPeerTask.Host.AddLoad(-1)
	}

	return
}

func (w *Worker) doSchedule(pr *scheduler2.PieceResult) (pkg *scheduler2.PiecePackage) {
	defer func() {
		if pr != nil {
			logger.Debugf("doSchedule %v, pkg[%v]", *pr, pkg)
		}
	} ()
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
	if peerTask == nil {
		return
	}
	pieceTaskList, waitingPieceNumList, err := w.scheduler.Scheduler(peerTask)
	if err != nil {
		switch err {
		case dferror.SchedulerWaitPiece:
			for _, pieceNum := range waitingPieceNumList {
				piece := peerTask.Task.GetOrCreatePiece(pieceNum)
				piece.AddWaitPeerTask(peerTask, func() {
					pr.ErrorCode = base.Code_SCHEDULER_RETRY_ERROR
					w.ReceiveJob(pr)
				})
			}
			return nil
		case dferror.SchedulerFinished:
			pkg = new(scheduler2.PiecePackage)
			pkg.TaskId = pr.TaskId
			pkg.Pid = pr.SrcPid
			pkg.Done = true
			pkg.ContentLength = peerTask.Task.ContentLength
		}
		return
	}
	// assemble result
	pkg = new(scheduler2.PiecePackage)
	pkg.TaskId = pr.TaskId
	pkg.Pid = pr.SrcPid
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
	w.sender.Send(pid, pkg)
	return
}
