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

package supervisor

import (
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
)

type TaskStatus uint8

func (status TaskStatus) String() string {
	switch status {
	case TaskStatusWaiting:
		return "Waiting"
	case TaskStatusRunning:
		return "Running"
	case TaskStatusSeeding:
		return "Seeding"
	case TaskStatusWaitingBackSource:
		return "WaitingBackSource"
	case TaskStatusSuccess:
		return "Success"
	case TaskStatusZombie:
		return "Zombie"
	case TaskStatusFailed:
		return "fail"
	case TaskStatusCDNRegisterFail:
		return "cdnRegisterFail"
	case TaskStatusSourceError:
		return "sourceError"
	default:
		return "unknown"
	}
}

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusSeeding
	TaskStatusWaitingBackSource
	TaskStatusSuccess
	TaskStatusZombie
	TaskStatusFailed
	TaskStatusCDNRegisterFail
	TaskStatusSourceError
)

type Task struct {
	lock            sync.RWMutex
	TaskID          string
	URL             string
	URLMeta         *base.UrlMeta
	DirectPiece     []byte
	CreateTime      time.Time
	lastAccessTime  time.Time
	lastTriggerTime time.Time
	pieceList       map[int32]*base.PieceInfo
	PieceTotal      int32
	ContentLength   int64
	status          TaskStatus
	peers           *sortedlist.SortedList
	// TODO add cdnPeers
}

func NewTask(taskID, url string, meta *base.UrlMeta) *Task {
	return &Task{
		TaskID:     taskID,
		URL:        url,
		URLMeta:    meta,
		CreateTime: time.Now(),
		pieceList:  make(map[int32]*base.PieceInfo),
		peers:      sortedlist.NewSortedList(),
		status:     TaskStatusWaiting,
	}
}

func (task *Task) AddPeer(peer *Peer) {
	task.peers.UpdateOrAdd(peer)
}

func (task *Task) UpdatePeer(peer *Peer) {
	task.peers.Update(peer)
}

func (task *Task) DeletePeer(peer *Peer) {
	task.peers.Delete(peer)
}

func (task *Task) ListPeers() *sortedlist.SortedList {
	return task.peers
}

func (task *Task) SetStatus(status TaskStatus) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.status = status
}

func (task *Task) GetStatus() TaskStatus {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.status
}

func (task *Task) GetPiece(pieceNum int32) *base.PieceInfo {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.pieceList[pieceNum]
}

func (task *Task) AddPiece(p *base.PieceInfo) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.pieceList[p.PieceNum] = p
}

func (task *Task) GetLastTriggerTime() time.Time {
	return task.lastTriggerTime
}

func (task *Task) UpdateLastTriggerTime(lastTriggerTime time.Time) {
	task.lastTriggerTime = lastTriggerTime
}

func (task *Task) Touch() {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.lastAccessTime = time.Now()
}

func (task *Task) GetLastAccessTime() time.Time {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.lastAccessTime
}

func (task *Task) UpdateTaskSuccessResult(pieceTotal int32, contentLength int64) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.status = TaskStatusSuccess
	task.PieceTotal = pieceTotal
	task.ContentLength = contentLength
}

func (task *Task) Lock() {
	task.lock.Lock()
}

func (task *Task) UnLock() {
	task.lock.Unlock()
}

func (task *Task) RLock() {
	task.lock.RLock()
}

func (task *Task) RUnlock() {
	task.lock.RUnlock()
}

const TinyFileSize = 128

// IsSuccess determines that whether cdn status is success.
func (task *Task) IsSuccess() bool {
	return task.status == TaskStatusSuccess
}

// IsFrozen determines that whether cdn status is frozen
func (task *Task) IsFrozen() bool {
	return task.status == TaskStatusWaiting || task.status == TaskStatusZombie || task.status == TaskStatusFailed ||
		task.status == TaskStatusSourceError || task.status == TaskStatusCDNRegisterFail
}

// CanSchedule determines whether task can be scheduled
// only task status is seeding or success can be scheduled
func (task *Task) CanSchedule() bool {
	return task.status == TaskStatusSeeding || task.status == TaskStatusSuccess
}

// IsWaiting determines whether task is waiting
func (task *Task) IsWaiting() bool {
	return task.status == TaskStatusWaiting
}

// IsHealth determines whether task is health
func (task *Task) IsHealth() bool {
	return task.status == TaskStatusRunning || task.status == TaskStatusSeeding || task.status == TaskStatusSuccess
}

// IsFail determines whether task is fail
func (task *Task) IsFail() bool {
	return task.status == TaskStatusFailed || task.status == TaskStatusSourceError || task.status == TaskStatusCDNRegisterFail
}
