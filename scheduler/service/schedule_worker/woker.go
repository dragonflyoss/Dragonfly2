package schedule_worker

import (
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
)

type Worker struct {
	jobChan chan *scheduler2.PieceResult
	sender ISender
	stopCh <-chan struct{}

	scheduler *scheduler.Scheduler
}

func CreateWorker(sche *scheduler.Scheduler, sender ISender, stop <-chan struct{}) *Worker {
	return &Worker{
		jobChan: make(chan *scheduler2.PieceResult, 10000),
		stopCh: stop,
		sender: sender,
		scheduler: sche,
	}
}

func(w *Worker) Start() {
	go w.doWorker()
}

func (w *Worker) ReceiveJob(job *scheduler2.PieceResult) {
	w.jobChan <- job
}

func (w *Worker) doWorker() {
	for {
		select{
		case  pr := <- w.jobChan:
			w.updatePieceResult(pr)
			pkg := w.doSchedule(pr)
			w.sendScheduleResult(pr.SrcPid, pkg)
		case <-w.stopCh:
			return
		}
	}
}

func (w *Worker) updatePieceResult(pr *scheduler2.PieceResult) {
	return
}

func (w *Worker) doSchedule(pr *scheduler2.PieceResult) (pkg *scheduler2.PiecePackage) {
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
	pieceTaskList, _ := w.scheduler.Scheduler(peerTask)
	// assemble result
	pkg = new(scheduler2.PiecePackage)
	pkg.TaskId = pr.TaskId
	if int(peerTask.GetFinishedNum()) + len(pieceTaskList) >= len(peerTask.Task.PieceList) {
		pkg.Last = true
		pkg.ContentLength = peerTask.Task.ContentLength
	}
	for _, p := range pieceTaskList {
		pkg.PieceTasks = append(pkg.PieceTasks, &scheduler2.PiecePackage_PieceTask{
			PieceNum : p.Piece.PieceNum,
			PieceRange : p.Piece.PieceRange,
			PieceMd5   : p.Piece.PieceMd5,
			SrcPid     : p.SrcPid ,
			DstPid     : p.DstPid,
			DstAddr    : p.DstAddr,
			PieceOffset: p.Piece.PieceOffset,
			PieceStyle : p.Piece.PieceStyle,
		})
	}
	return
}

func (w *Worker) sendScheduleResult(pid string, pkg *scheduler2.PiecePackage) {
	w.sender.Send(pid, pkg)
	return
}
