/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

import (
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/metrics"
)

type Task struct {
	TaskID string `json:"task_id,omitempty"`
	URL    string `json:"url,omitempty"`
	// regex format, used for task id generator, assimilating different urls
	Filter string `json:"filter,omitempty"`
	// biz_id and md5 are used for task id generator to distinguish the same urls
	// md5 is also used to check consistency about file content
	BizID   string        `json:"biz_id,omitempty"`   // caller's biz id that can be any string
	URLMata *base.UrlMeta `json:"url_mata,omitempty"` // downloaded file content md5

	SizeScope   base.SizeScope
	DirectPiece *scheduler.RegisterResult_PieceContent

	CreateTime    time.Time
	LastActive    time.Time
	rwLock        sync.RWMutex
	PieceList     map[int32]*Piece // Piece list
	PieceTotal    int32            // the total number of Pieces, set > 0 when cdn finished
	ContentLength int64
	Statistic     *metrics.TaskStatistic
	Removed       bool
	CDNError      *dferrors.DfError
}

func CopyTask(t *Task) *Task {
	copyTask := *t
	if copyTask.PieceList == nil {
		copyTask.PieceList = make(map[int32]*Piece)
		copyTask.CreateTime = time.Now()
		copyTask.LastActive = copyTask.CreateTime
		copyTask.SizeScope = base.SizeScope_NORMAL
		copyTask.Statistic = &metrics.TaskStatistic{
			StartTime: time.Now(),
		}
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
