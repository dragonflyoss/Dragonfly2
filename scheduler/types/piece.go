package types

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type Piece struct {
	PieceNum   int32  `json:"piece_num,omitempty"`
	PieceRange string `json:"piece_range,omitempty"`
	PieceMd5   string `json:"piece_md5,omitempty"`

	PieceOffset uint64          `json:"piece_offset,omitempty"`
	PieceStyle  base.PieceStyle `json:"piece_style,omitempty"`

	Task                    *Task
	readyPeerTaskList       *sync.Map
	downloadingPeerTaskList *sync.Map
}

func (p *Piece) GetReadyPeerTaskList() (list []*PeerTask) {
	if p == nil || p.readyPeerTaskList == nil {
		return
	}
	var downPeerTaskList []string
	p.readyPeerTaskList.Range(func(key, value interface{}) bool {
		host := value.(*PeerTask)
		if host.IsDown() {
			downPeerTaskList = append(downPeerTaskList, host.Pid)
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
	if p.readyPeerTaskList == nil {
		p.readyPeerTaskList = new(sync.Map)
	}
	p.readyPeerTaskList.Store(pt.Pid, pt)
}

type PieceTask struct {
	Piece   *Piece
	SrcPid  string
	DstPid  string
	DstAddr string  // ip:port
}
