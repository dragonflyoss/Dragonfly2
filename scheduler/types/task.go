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

	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
)

type TaskStatus uint8

const (
	TaskStatusWaiting TaskStatus = iota + 1
	TaskStatusRunning
	TaskStatusFailed
	TaskStatusSuccess
	TaskStatusSourceError
)

// isSuccessCDN determines that whether the CDNStatus is success.
func IsSuccess(status TaskStatus) bool {
	return status == TaskStatusSuccess
}

func IsFrozen(status TaskStatus) bool {
	return status == TaskStatusFailed ||
		status == TaskStatusWaiting ||
		status == TaskStatusSourceError
}

func IsWait(status TaskStatus) bool {
	return status == TaskStatusWaiting
}

func IsBadTask(status TaskStatus) bool {
	return status == TaskStatusFailed
}

type Task struct {
	lock            sync.RWMutex
	TaskID          string
	URL             string
	Filter          string
	BizID           string
	UrlMeta         *base.UrlMeta
	DirectPiece     []byte
	CreateTime      time.Time
	LastAccessTime  time.Time
	LastTriggerTime time.Time
	PieceList       map[int32]*PieceInfo
	PieceTotal      int32
	ContentLength   int64
	Status          TaskStatus
	PeerNodes       sortedlist.SortedList
}

func NewTask(taskID, url, filter, bizID string, meta *base.UrlMeta) *Task {
	return &Task{
		TaskID:  taskID,
		URL:     url,
		Filter:  filter,
		BizID:   bizID,
		UrlMeta: meta,
		Status:  TaskStatusWaiting,
	}
}

func (task *Task) GetPiece(pieceNum int32) *PieceInfo {
	task.lock.RLock()
	defer task.lock.RUnlock()
	task.LastAccessTime = time.Now()
	return task.PieceList[pieceNum]
}

func (task *Task) AddPeerNode(peer *PeerNode) {
	task.lock.Lock()
	defer task.lock.RUnlock()
	task.LastAccessTime = time.Now()
	task.PeerNodes.UpdateOrAdd(peer)
}

func (task *Task) AddPiece(p *PieceInfo) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.LastAccessTime = time.Now()
	task.PieceList[p.PieceNum] = p
}

type PieceInfo struct {
	PieceNum    int32
	RangeStart  uint64
	RangeSize   int32
	PieceMd5    string
	PieceOffset uint64
	PieceStyle  int8
}
