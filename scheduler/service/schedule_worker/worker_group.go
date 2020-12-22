package schedule_worker

import (
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"hash/crc32"
)

type IWorker interface {
	Start()
	Stop()
	ReceiveJob(job *scheduler2.PieceResult)
}

type WorkerGroup struct {
	workerNum  int
	chanSize   int
	workerList []*Worker
	stopCh     chan struct{}
	sender     ISender

	scheduler *scheduler.Scheduler
}

func CreateWorkerGroup(scheduler *scheduler.Scheduler) *WorkerGroup {
	workerNum := config.GetConfig().Worker.WorkerNum
	chanSize := config.GetConfig().Worker.WorkerJobPoolSize
	return &WorkerGroup{
		workerNum: workerNum,
		chanSize:  chanSize,
		sender:    CreateSender(),
		scheduler: scheduler,
	}
}

func (wg *WorkerGroup) Start() {
	wg.stopCh = make(chan struct{})
	for i := 0; i < wg.workerNum; i++ {
		w := CreateWorker(wg.scheduler, wg.sender, wg.chanSize, wg.stopCh)
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
