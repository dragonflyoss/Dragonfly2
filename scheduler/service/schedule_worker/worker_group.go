package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"hash/crc32"
	"time"
)

type IWorker interface {
	Start()
	Stop()
	ReceiveJob(job *types.PeerTask)
	TriggerSchedule(*types.PeerTask)
}

type WorkerGroup struct {
	workerNum  int
	chanSize   int
	workerList []*Worker
	stopCh     chan struct{}
	sender     ISender

	triggerLoadChan chan *types.PeerTask

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
		triggerLoadChan: make(chan *types.PeerTask, 1000),
	}
}

func (wg *WorkerGroup) Start() {
	wg.stopCh = make(chan struct{})
	for i := 0; i < wg.workerNum; i++ {
		w := CreateWorker(wg.scheduler, wg.sender, wg.stopCh)
		w.Start()
		wg.workerList = append(wg.workerList, w)
	}
	wg.sender.Start()
	go wg.triggerScheduleLoop()
	logger.Infof("start scheduler worker number:%d", wg.workerNum)
}

func (wg *WorkerGroup) Stop() {
	close(wg.stopCh)
	wg.sender.Stop()
	logger.Infof("stop scheduler worker")
}

func (wg *WorkerGroup) ReceiveJob(job *types.PeerTask) {
	if job == nil {
		return
	}
	choiceWorkerId := crc32.ChecksumIEEE([]byte(job.Pid)) % uint32(wg.workerNum)
	wg.workerList[choiceWorkerId].ReceiveJob(job)
}

func (wg *WorkerGroup) TriggerSchedule(pt *types.PeerTask) {
	select {
		case wg.triggerLoadChan <- pt:
		default:
	}
}

func (wg *WorkerGroup) triggerScheduleLoop() {
	peerTaskMgr := mgr.GetPeerTaskManager()
	ticker := time.NewTicker(time.Second*10)
	defer ticker.Stop()
	for {
		select {
		case <-wg.stopCh:
			break
		case pt := <-wg.triggerLoadChan:
			if pt != nil {
				pt.TriggerSchedule(1)
			}
		case <-ticker.C:
			peerTaskMgr.Walker(func(pt *types.PeerTask) bool {
				load := pt.Host.GetLoad()
				if load <= 1 {
					pt.TriggerSchedule(1)
				}
				return true
			})
		}
	}
}
