package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"sync"
	"sync/atomic"
	"time"
)

type PeerTaskStatus int8

const (
	PeerTaskStatusHealth  PeerTaskStatus = 0
	PeerTaskStatusNeedParent  PeerTaskStatus = 1
	PeerTaskStatusNeedChildren   PeerTaskStatus = 2
	PeerTaskStatusBadNode    PeerTaskStatus = 3
	PeerTaskStatusNeedAdjustNode PeerTaskStatus = 4
)

type PeerTask struct {
	Pid  string // peer id
	Task *Task  // task info
	Host *Host  // host info

	isDown          bool // is leave scheduler
	pieceStatusList *sync.Map
	lock            *sync.Mutex
	finishedNum     int32 // download finished piece number
	lastActiveTime  int64
	touch           func(*PeerTask)

	parent          *PeerEdge               // primary download provider
	children        map[*PeerTask]*PeerEdge // all primary download consumers
	subTreeNodesNum int32                   // node number of subtree and current node is root of the subtree
	deep            int32                   // the node number of the path from root to the current node

	// the client of peer task, which used for send and receive msg
	client scheduler.Scheduler_PullPieceTasksServer

	Traffic   uint64
	Cost      uint32
	Success   bool
	ErrorCode base.Code

	status PeerTaskStatus
}

type PeerEdge struct {
	SrcPeerTask *PeerTask // child, consumer
	DstPeerTask *PeerTask // parent, provider
	Concurrency int8      // number of thread download from the provider
	CostHistory []int32   // history of downloading one piece cost from the provider
}

type PieceStatus struct {
	PieceNum  int32
	SrcPid    string
	DstPid    string
	Success   bool
	ErrorCode base.Code
	Cost      uint32
}

func NewPeerTask(pid string, task *Task, host *Host, touch func(*PeerTask)) *PeerTask {
	pt := &PeerTask{
		Pid:             pid,
		Task:            task,
		Host:            host,
		pieceStatusList: new(sync.Map),
		isDown:          false,
		lock:            new(sync.Mutex),
		lastActiveTime:  time.Now().UnixNano(),
		subTreeNodesNum: 1,
		children:        make(map[*PeerTask]*PeerEdge),
		touch:           touch,
	}
	host.AddPeerTask(pt)
	pt.Touch()
	return pt
}

func (pt *PeerTask) AddParent(parent *PeerTask, concurrency int8) {
	if pt == nil || parent == nil {
		return
	}

	pe := &PeerEdge{
		SrcPeerTask: pt,     // child
		DstPeerTask: parent, // parent
		Concurrency: concurrency,
	}
	pt.parent = pe
	parent.children[pt] = pe

	// modify subTreeNodesNum of all ancestor
	p, c := parent, pt
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, c.subTreeNodesNum)
		c = p
		if p.parent == nil || p.parent.DstPeerTask == nil {
			break
		}
		p = p.parent.DstPeerTask
	}

	pt.Host.AddDownloadLoad(int32(concurrency))
	if parent.Host != nil {
		parent.Host.AddUploadLoad(int32(concurrency))
	}
}

func (pt *PeerTask) AddConcurrency(parent *PeerTask, delta int8) {
	if pt == nil || parent == nil {
		return
	}

	if pt.parent == nil && pt.parent.DstPeerTask != parent {
		return
	}

	pt.parent.Concurrency += delta

	pt.Host.AddDownloadLoad(int32(delta))
	parent.Host.AddUploadLoad(int32(delta))
}

func (pt *PeerTask) DeleteParent(parent *PeerTask) {
	if pt == nil || parent == nil {
		return
	}

	if pt.parent != nil && pt.parent.DstPeerTask == parent {
		pt.parent = nil
	}

	delete(parent.children, pt)

	p, c := parent, pt
	for p != nil {
		atomic.AddInt32(&p.subTreeNodesNum, -c.subTreeNodesNum)
		c = p
		if p.parent == nil || p.parent.DstPeerTask == nil {
			break
		}
		p = p.parent.DstPeerTask
	}

	pt.Host.AddDownloadLoad(-1)
	parent.Host.AddUploadLoad(-1)
}

func (pt *PeerTask) GetFreeLoad() int32 {
	if pt.Host == nil {
		return 0
	}
	return pt.Host.GetFreeUploadLoad()
}

func (pt *PeerTask) Touch() {
	pt.lastActiveTime = time.Now().UnixNano()
	pt.touch(pt)
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
		return
	}
	pt.finishedNum++
	piece := pt.Task.GetOrCreatePiece(ps.PieceNum)
	if piece != nil {
		piece.AddReadyPeerTask(pt)
	}
}

func (pt *PeerTask) IsDown() (ok bool) {
	return pt.isDown
}

func (pt *PeerTask) SetDown() {
	pt.isDown = true
	pt.Touch()
}

func (pt *PeerTask) SetUp() {
	pt.isDown = false
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

func (pt *PeerTask) Send() error {
	// if pt != nil && pt.client != nil {
	var pkg *scheduler.PiecePackage
	if pt.client != nil {
		return pt.client.Send(pkg)
	}
	return nil
}

func (pt *PeerTask) GetDiffPieceNum(dst *PeerTask) int32 {
	diff := dst.finishedNum - pt.finishedNum
	if diff > 0 {
		return diff
	}
	return 0
}

func (pt *PeerTask) GetSubTreeNodesNum() int32 {
	return pt.subTreeNodesNum
}

func (pt *PeerTask) GetDeep() int32 {
	return pt.deep
}

func (pt *PeerTask) GetNodeStatus() PeerTaskStatus {
	return pt.status
}

func (pt *PeerTask) SetNodeStatus(status PeerTaskStatus) {
	pt.status = status
}
