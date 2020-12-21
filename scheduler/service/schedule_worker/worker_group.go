package schedule_worker

import (
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"hash/crc32"
)

type IWorker interface {
	Start()
	Stop()
	ReceiveJob(job *scheduler2.PieceResult)
}

type WorkerGroup struct {
	workerNum int
	workerList []*Worker
	stopCh chan struct{}
	sender ISender

	scheduler *scheduler.Scheduler
}

func CreateWorkerGroup(scheduler *scheduler.Scheduler) *WorkerGroup {
	return &WorkerGroup{
		workerNum: 2,
		sender: CreateSender(),
		scheduler: scheduler,
	}
}

func (wg *WorkerGroup) Start() {
	wg.stopCh = make(chan struct{})
	for i:=0; i<wg.workerNum; i++ {
		w := CreateWorker(wg.scheduler, wg.sender, wg.stopCh)
		w.Start()
		wg.workerList = append(wg.workerList, w)
	}
}

func (wg *WorkerGroup) Stop() {
	close(wg.stopCh)
}

func (wg *WorkerGroup) ReceiveJob(job *scheduler2.PieceResult) {
	if job == nil {
		return
	}
	choiceWorkerId := crc32.ChecksumIEEE([]byte(job.SrcPid)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerId].ReceiveJob(job)
}



