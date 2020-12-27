package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"sync"
	"time"
)

type PeerTask struct {
	Pid  string // peer id
	Task *Task  // task info
	Host *Host  // host info

	isDown                  bool // is leave scheduler
	downloadingPieceNumList *sync.Map
	pieceStatusList         *sync.Map
	retryPieceList          map[int32]int32
	lock                    *sync.Mutex
	firstPieceNum           int32 //
	finishedNum             int32 // download finished piece number
	lastActiveTime 			int64

	// the client of peer task, which used for send and receive msg
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
		retryPieceList:          make(map[int32]int32),
		isDown:                  false,
		lock:                    new(sync.Mutex),
		lastActiveTime: time.Now().UnixNano(),
	}
	host.AddPeerTask(pt)
	return pt
}

func (pt *PeerTask) GetRetryPieceList() map[int32]int32 {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	ret := make(map[int32]int32, len(pt.retryPieceList))
	for k, v := range pt.retryPieceList {
		ret[k] = v
	}
	return ret
}

func (pt *PeerTask) Touch() {
	pt.lastActiveTime = time.Now().UnixNano()
}

func (pt *PeerTask) GetFirstPieceNum() int32 {
	return pt.firstPieceNum
}

func (pt *PeerTask) GetFinishedNum() int32 {
	return pt.finishedNum
}

func (pt *PeerTask) GetPieceStatusList() *sync.Map {
	return pt.pieceStatusList
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
		pt.retryPieceList[ps.PieceNum]++
		return
	}
	pt.finishedNum++
	delete(pt.retryPieceList, ps.PieceNum)
	piece := pt.Task.GetOrCreatePiece(ps.PieceNum)
	if piece != nil {
		piece.AddReadyPeerTask(pt)
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

func (pt *PeerTask) AddDownloadingPiece(pieceNum int32) {
	pt.downloadingPieceNumList.Store(pieceNum, true)
	return
}

func (pt *PeerTask) DeleteDownloadingPiece(pieceNum int32) {
	pt.downloadingPieceNumList.Delete(pieceNum)
	return
}

func (pt *PeerTask) IsPieceDownloading(num int32) (ok bool) {
	_, ok = pt.downloadingPieceNumList.Load(num)
	return
}

func (pt *PeerTask) GetDownloadingPieceNum() (num int32) {
	pt.downloadingPieceNumList.Range(func(key, value interface{}) bool {
		num++
		return true
	})
	return
}

func (pt *PeerTask) IsDown() (ok bool) {
	return pt.isDown
}

func (pt *PeerTask) SetDown() {
	pt.isDown = true
	pt.Touch()
}

func (pt *PeerTask) SetStatus(traffic uint64, cost uint32, success bool, errorCode base.Code) {
	pt.Traffic = traffic
	pt.Cost = cost
	pt.Success = success
	pt.ErrorCode = errorCode
	pt.Touch()
}

func (pt *PeerTask) SetClient(client scheduler.Scheduler_PullPieceTasksServer) {
	pt.client = client
}

func (pt *PeerTask) Send(pkg *scheduler.PiecePackage) error {
	// if pt != nil && pt.client != nil {
	if pt.client != nil {
		return pt.client.Send(pkg)
	}
	return nil
}
