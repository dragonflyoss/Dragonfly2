package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"hash/crc32"
	"k8s.io/client-go/util/workqueue"
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

	triggerLoadQueue workqueue.Interface

	scheduler *scheduler.Scheduler
}

func CreateWorkerGroup(scheduler *scheduler.Scheduler) *WorkerGroup {
	workerNum := config.GetConfig().Worker.WorkerNum
	chanSize := config.GetConfig().Worker.WorkerJobPoolSize
	return &WorkerGroup{
		workerNum:       workerNum,
		chanSize:        chanSize,
		sender:          CreateSender(),
		scheduler:       scheduler,
		triggerLoadQueue: workqueue.New(),
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
	wg.triggerLoadQueue.ShutDown()
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
	wg.triggerLoadQueue.Add(pt)
}

func (wg *WorkerGroup) triggerScheduleLoop() {
	go wg.triggerScheduleTicker()

	for {
		it, shutdown := wg.triggerLoadQueue.Get()
		if shutdown {
			break
		}
		pt, _ := it.(*types.PeerTask)
		if pt != nil {
			pt.TriggerSchedule(2)
		}
		wg.triggerLoadQueue.Done(pt)
	}
}

func (wg *WorkerGroup) triggerScheduleTicker() {
	peerTaskMgr := mgr.GetPeerTaskManager()
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-wg.stopCh:
			break
		case <-ticker.C:
			// trigger all waiting peer task periodic
			if wg.triggerLoadQueue.ShuttingDown() {
				break
			}
			peerTaskMgr.Walker(func(pt *types.PeerTask) bool {
				load := pt.Host.GetLoad()
				if load <= 2 {
				 	wg.triggerLoadQueue.Add(pt)
				}
				return true
			})
		}
	}
}
