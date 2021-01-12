package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type WaitingType int

type Piece struct {
	PieceNum   int32  `json:"piece_num,omitempty"`
	PieceRange string `json:"piece_range,omitempty"`
	PieceMd5   string `json:"piece_md5,omitempty"`

	PieceOffset uint64          `json:"piece_offset,omitempty"`
	PieceStyle  base.PieceStyle `json:"piece_style,omitempty"`

	Task               *Task
}

func newEmptyPiece(pieceNum int32, task *Task) *Piece {
	return &Piece{
		PieceNum:          pieceNum,
		Task:              task,
	}
}
