package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"sync"
)

type PeerTask struct {
	Pid  string `json:"pid,omitempty"`       // peer id
	Task *Task  `json:"peer_host,omitempty"` // task info
	Host *Host  `json:"peer_host,omitempty"` // host info

	isDown                  bool // is leave scheduler
	downloadingPieceNumList *sync.Map
	pieceStatusList         *sync.Map
	lock                    *sync.Mutex
	firstPieceNum           int32 //
	finishedNum             int32 // download finished piece number

	client scheduler.Scheduler_PullPieceTasksServer

	Traffic   uint64
	Cost      uint32
	Success   bool
	ErrorCode base.Code
}

type PieceStatus struct {
	PieceNum  int32
	SrcPid    string
	DstPid    string
	Success   bool
	ErrorCode base.Code
	Cost      uint32
}

func NewPeerTask(pid string, task *Task, host *Host) *PeerTask {
	pt := &PeerTask{
		Pid:                     pid,
		Task:                    task,
		Host:                    host,
		downloadingPieceNumList: new(sync.Map),
		pieceStatusList:         new(sync.Map),
		isDown:                  false,
		lock:                    new(sync.Mutex),
	}
	host.AddPeerTask(pt)
	return pt
}

func (pt *PeerTask) GetFirstPieceNum() int32 {
	return pt.firstPieceNum
}

func (pt *PeerTask) GetFinishedNum() int32 {
	return pt.finishedNum
}

func (pt *PeerTask) AddPieceStatus(ps *PieceStatus) {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	if pt.pieceStatusList == nil {
		pt.pieceStatusList = new(sync.Map)
	}
	old, loaded := pt.pieceStatusList.LoadOrStore(ps.PieceNum, ps)
	if loaded {
		oldPs, _ := old.(*PieceStatus)
		if oldPs != nil && oldPs.Success {
			return
		}
		if ps.Success {
			pt.pieceStatusList.Store(ps.PieceNum, ps)
		}
	}
	if !ps.Success {
		return
	}
	for {
		v, ok := pt.pieceStatusList.Load(pt.firstPieceNum)
		if !ok {
			break
		}
		tps, _ := v.(*PieceStatus)
		if tps == nil || !tps.Success {
			break
		}
		pt.firstPieceNum++
	}
}

func (pt *PeerTask) IsPieceDownloading(num int32) (ok bool) {
	_, ok = pt.downloadingPieceNumList.Load(num)
	return
}

func (pt *PeerTask) IsDown() (ok bool) {
	return pt.isDown
}

func (pt *PeerTask) SetDown() {
	pt.isDown = true
}

func (pt *PeerTask) SetStatus(traffic uint64, cost uint32, success bool, errorCode base.Code) {
	pt.Traffic = traffic
	pt.Cost = cost
	pt.Success = success
	pt.ErrorCode = errorCode
}

func (pt *PeerTask) SetClient(client scheduler.Scheduler_PullPieceTasksServer) {
	pt.client = client
}

func (pt *PeerTask) Send(pkg *scheduler.PiecePackage) error {
	return pt.client.Send(pkg)
}
