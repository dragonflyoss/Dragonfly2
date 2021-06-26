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
	"time"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
)

type Task struct {
	TaskID        string
	URL           string
	Filter        string
	BizID         string
	URLMata       *base.UrlMeta
	SizeScope     base.SizeScope
	DirectPiece   byte[]
	CreateTime    time.Time
	LastActive    time.Time
	PieceList     map[int32]*Piece
	PieceTotal    int32
	ContentLength int64
	Statistic     *daemon.TaskStatistic
	CDNError      *dferrors.DfError
}

func (t *Task) InitProps() {
	if t.PieceList == nil {
		t.CreateTime = time.Now()
		t.LastActive = t.CreateTime
		t.SizeScope = base.SizeScope_NORMAL
		t.Statistic = &daemon.TaskStatistic{
			StartTime: time.Now(),
		}
	}
}

func (t *Task) GetPiece(pieceNum int32) *Piece {
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
	t.PieceList[p.PieceNum] = p
}
