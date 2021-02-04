package types

import (
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"sync"
	"time"
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

	SizeScope base.SizeScope
	DirectPiece *scheduler.RegisterResult_PieceContent

	CreateTime    time.Time
	rwLock        *sync.RWMutex
	PieceList     map[int32]*Piece // Piece list
	PieceTotal    int32            // the total number of Pieces, set > 0 when cdn finished
	ContentLength int64
}

func CopyTask(t *Task) *Task {
	copyTask := *t
	if copyTask.PieceList == nil {
		copyTask.PieceList = make(map[int32]*Piece)
		copyTask.rwLock = new(sync.RWMutex)
		copyTask.CreateTime = time.Now()
		copyTask.SizeScope = base.SizeScope_NORMAL
	}
	return &copyTask
}

func (t *Task) GetPiece(pieceNum int32) *Piece {
	t.rwLock.RLock()
	defer t.rwLock.RUnlock()
	return t.PieceList[pieceNum]
}

func (t *Task) GetOrCreatePiece(pieceNum int32) *Piece {
	t.rwLock.RLock()
	p := t.PieceList[pieceNum]
	if p == nil {
		t.rwLock.RUnlock()
		p = newEmptyPiece(pieceNum, t)
		t.rwLock.Lock()
		t.PieceList[pieceNum] = p
		t.rwLock.Unlock()
	} else {
		t.rwLock.RUnlock()
	}
	return p
}

func (t *Task) AddPiece(p *Piece) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	t.PieceList[p.PieceNum] = p
}
