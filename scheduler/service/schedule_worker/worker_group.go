package schedule_worker

import (
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	scheduler2 "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"hash/crc32"
	"k8s.io/client-go/util/workqueue"
)

type IWorker interface {
	Start()
	Stop()
	ReceiveJob(job *types.PeerTask)
	ReceiveUpdatePieceResult(pr *scheduler2.PieceResult)
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
		workerNum:        workerNum,
		chanSize:         chanSize,
		sender:           CreateSender(),
		scheduler:        scheduler,
		triggerLoadQueue: workqueue.New(),
	}
}

func (wg *WorkerGroup) Start() {
	wg.stopCh = make(chan struct{})

	mgr.GetPeerTaskManager().SetDownloadingMonitorCallBack(func(pt *types.PeerTask) {
		if pt.GetNodeStatus() != types.PeerTaskStatusHealth {
		} else if pt.GetParent() == nil {
			pt.SetNodeStatus(types.PeerTaskStatusNeedParent)
		} else {
			pt.SetNodeStatus(types.PeerTaskStatusNeedCheckNode)
		}
		wg.ReceiveJob(pt)
	})

	for i := 0; i < wg.workerNum; i++ {
		w := CreateWorker(wg.scheduler, wg.sender, wg.ReceiveJob, wg.stopCh)
		w.Start()
		wg.workerList = append(wg.workerList, w)
	}
	wg.sender.Start()
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
	var choiceWorkerId uint32
	if job.Host == nil {
		choiceWorkerId = crc32.ChecksumIEEE([]byte(job.Pid)) % uint32(wg.workerNum)
	} else {
		choiceWorkerId = crc32.ChecksumIEEE([]byte(job.Host.Uuid)) % uint32(wg.workerNum)
	}
	wg.workerList[choiceWorkerId].ReceiveJob(job)
}

func (wg *WorkerGroup) ReceiveUpdatePieceResult(pr *scheduler2.PieceResult) {
	if pr == nil {
		return
	}
	var choiceWorkerId uint32
	pt, _ := mgr.GetPeerTaskManager().GetPeerTask(pr.SrcPid)
	if pt == nil || pt.Host == nil {
		choiceWorkerId = crc32.ChecksumIEEE([]byte(pr.SrcPid)) % uint32(wg.workerNum)
	} else {
		choiceWorkerId = crc32.ChecksumIEEE([]byte(pt.Host.Uuid)) % uint32(wg.workerNum)
	}
	wg.workerList[choiceWorkerId].ReceiveUpdatePieceResult(pr)
}
