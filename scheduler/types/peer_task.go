package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"sync"
	"sync/atomic"
)

type PeerTask struct {
	Pid  string `json:"pid,omitempty"`       // peer id
	Task *Task  `json:"peer_host,omitempty"` // task info
	Host *Host  `json:"peer_host,omitempty"` // host info

	isDown bool // is leave scheduler
	downloadingPieceNumList *sync.Map
	pieceStatusList *sync.Map
	firstPieceNum *int32 //
	finishedNum *int32 // download finished piece number

	client scheduler.Scheduler_PullPieceTasksServer

	Traffic        uint64
	Cost           uint32
	Success        bool
	ErrorCode      base.Code
}

type PieceStatus struct {
	SrcPid     string    `protobuf:"bytes,2,opt,name=src_pid,json=srcPid,proto3" json:"src_pid,omitempty"`
	Success    bool      `protobuf:"varint,6,opt,name=success,proto3" json:"success,omitempty"`
	ErrorCode  base.Code `protobuf:"varint,7,opt,name=error_code,json=errorCode,proto3,enum=base.Code" json:"error_code,omitempty"` // for success is false
	Cost       uint32    `protobuf:"varint,8,opt,name=cost,proto3" json:"cost,omitempty"`
}

func NewPeerTask(pid string, task *Task, host *Host) *PeerTask {
	return &PeerTask{
		Pid: pid,
		Task: task,
		Host: host,
		downloadingPieceNumList: new(sync.Map),
		pieceStatusList: new(sync.Map),
		firstPieceNum: new(int32),
		finishedNum: new(int32),
		isDown: false,
	}
}

func (pt *PeerTask) GetFirstPieceNum() int32 {
	return atomic.LoadInt32(pt.firstPieceNum)
}

func (pt *PeerTask) GetFinishedNum() int32 {
	return atomic.LoadInt32(pt.finishedNum)
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

func (pt *PeerTask) SetStatus(traffic uint64, cost uint32, success bool, errorCode base.Code ) {
	pt.Traffic = traffic
	pt.Cost = cost
	pt.Success = success
	pt.ErrorCode = errorCode
}

func (pt *PeerTask) SetClient(client scheduler.Scheduler_PullPieceTasksServer) {
	pt.client = client
}

func (pt *PeerTask) Send(pkg *scheduler.PiecePackage) {
	pt.client.Send(pkg)
}


