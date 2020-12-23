package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"sync"
)

type Task struct {
	TaskId string `json:"task_id,omitempty"`
	Url    string `json:"url,omitempty"`
	// regex format, used for task id generator, assimilating different urls
	Filter string `json:"filter,omitempty"`
	// biz_id and md5 are used for task id generator to distinguish the same urls
	// md5 is also used to check consistency about file content
	BizId   string        `json:"biz_id,omitempty"`   // caller's biz id that can be any string
	UrlMata *base.UrlMeta `json:"url_mata,omitempty"` // downloaded file content md5

	rwLock        *sync.RWMutex
	PieceList     map[int32]*Piece // Piece list
	maxPieceNum   int32            // the max piece num of all pieces
	PieceTotal    int32            // the total number of Pieces
	ContentLength int64
}

func CopyTask(t *Task) *Task {
	copyTask := *t
	if copyTask.PieceList == nil {
		copyTask.PieceList = make(map[int32]*Piece)
		copyTask.rwLock = new(sync.RWMutex)
		copyTask.maxPieceNum = -1
	}
	return &copyTask
}

func (t *Task) GetMaxPieceNum() int32 {
	return t.maxPieceNum
}

func (t *Task) GetPiece(pieceNum int32) *Piece {
	t.rwLock.RLock()
	defer t.rwLock.RUnlock()
	return t.PieceList[pieceNum]
}

func (t *Task) AddPiece(p *Piece) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	t.PieceList[p.PieceNum] = p
	if p.PieceNum > t.maxPieceNum {
		t.maxPieceNum = p.PieceNum
	}
}
