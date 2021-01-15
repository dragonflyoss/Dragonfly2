package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type WaitingType int

type Piece struct {
	base.PieceTask

	Task *Task
}

func newEmptyPiece(pieceNum int32, task *Task) *Piece {
	return &Piece{
		PieceTask: base.PieceTask{PieceNum: pieceNum},
		Task:      task,
	}
}
