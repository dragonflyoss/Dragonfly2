package types

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type WaitingType int

type Piece struct {
	PieceNum   int32  `json:"piece_num,omitempty"`
	PieceRange string `json:"piece_range,omitempty"`
	PieceMd5   string `json:"piece_md5,omitempty"`

	PieceOffset uint64          `json:"piece_offset,omitempty"`
	PieceStyle  base.PieceStyle `json:"piece_style,omitempty"`

	Task                    *Task
	readyPeerTaskList       *sync.Map
	waitingPeerTask 		*sync.Map
	isReady 				bool
}

func newEmptyPiece(pieceNum int32, task *Task) *Piece {
	return &Piece{
		PieceNum: pieceNum,
		Task: task,
		isReady: false,
		readyPeerTaskList:  new(sync.Map),
		waitingPeerTask:  new(sync.Map),
	}
}

func (p *Piece) GetReadyPeerTaskList() (list []*PeerTask) {
	if p == nil {
		return
	}
	var downPeerTaskList []*string
	p.readyPeerTaskList.Range(func(key, value interface{}) bool {
		host := value.(*PeerTask)
		if host.IsDown() {
			downPeerTaskList = append(downPeerTaskList, &host.Pid)
			return true
		}
		list = append(list, host)
		return true
	})
	for _, pid := range downPeerTaskList {
		p.readyPeerTaskList.Delete(pid)
	}
	return
}

func (p *Piece) AddReadyPeerTask(pt *PeerTask) {
	p.readyPeerTaskList.Store(&pt.Pid, pt)
	if p.waitingPeerTask != nil {
		p.waitingPeerTask.Range(func(key interface{}, value interface{}) bool {
			f, ok := value.(func())
			if ok {
				f()
			}
			p.waitingPeerTask.Delete(key)
			p.isReady = true
			return true
		})
	}
}

func (p *Piece) AddWaitPeerTask(peerTask *PeerTask, callback func()) {
	p.waitingPeerTask.Store(peerTask, callback)
}

type PieceTask struct {
	Piece   *Piece
	SrcPid  string
	DstPid  string
	DstAddr string  // ip:port
}
