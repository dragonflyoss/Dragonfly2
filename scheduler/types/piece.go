package types

import (
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/base"
)

type WaitingType int

type Piece struct {
	base.PieceInfo

	Task *Task
}

func newEmptyPiece(pieceNum int32, task *Task) *Piece {
	return &Piece{
		PieceInfo: base.PieceInfo{PieceNum: pieceNum},
		Task:      task,
	}
}
