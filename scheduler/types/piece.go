package types

import (
	"sync"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
)

type Piece struct {
	PieceNum    int32           `json:"piece_num,omitempty"`
	PieceRange  string          `json:"piece_range,omitempty"`
	PieceMd5    string          `json:"piece_md5,omitempty"`

	PieceOffset uint64          `json:"piece_offset,omitempty"`
	PieceStyle  base.PieceStyle `json:"piece_style,omitempty"`

	Task *Task
	readyPeerHostList *sync.Map
	downloadingPeerHostList *sync.Map
}

func(p *Piece) GetReadPeerHostList() (list []*PeerHost) {
	if p == nil || p.readyPeerHostList == nil {
		return
	}
	p.readyPeerHostList.Range(func(key, value interface{}) bool {
		host := value.(*PeerHost)
		list = append(list, host)
		return true
	})
	return
}

type PieceTask struct {
	Piece *Piece
	SrcPid      string          `json:"src_pid,omitempty"`
	DstPid      string          `json:"dst_pid,omitempty"`
	DstAddr     string          `json:"dst_addr,omitempty"` // ip:port
}