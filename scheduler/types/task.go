package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
)

type Task struct {
	TaskId string `json:"task_id,omitempty"`
	Url string `json:"url,omitempty"`
	// regex format, used for task id generator, assimilating different urls
	Filter string `json:"filter,omitempty"`
	// biz_id and md5 are used for task id generator to distinguish the same urls
	// md5 is also used to check consistency about file content
	BizId   string        `json:"biz_id,omitempty"`   // caller's biz id that can be any string
	UrlMata *base.UrlMeta `json:"url_mata,omitempty"` // downloaded file content md5

	PieceList []*Piece // Piece 列表
	PieceNum int32 // Piece总数
	ContentLength uint64
}

func CopyTask(t *Task) *Task {
	copyTask := *t
	return &copyTask
}

func (t *Task) GetPieceTotal() int32 {
	return int32(len(t.PieceList))
}

func (t *Task) GetPiece(pieceNum int32) *Piece {
	return t.PieceList[pieceNum]
}