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
//go:generate mockgen -destination ./mocks/task_mock.go -package mocks d7y.io/dragonfly/v2/scheduler/supervisor TaskManager

package supervisor

import (
	"sync"
	"time"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/list"
	gc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	TaskGCID     = "task"
	TinyFileSize = 128
)

type TaskManager interface {
	// Add task
	Add(*Task)
	// Get task
	Get(string) (*Task, bool)
	// Delete task
	Delete(string)
	// GetOrAdd or add task
	GetOrAdd(*Task) (*Task, bool)
}

type taskManager struct {
	// peerManager is peer manager
	peerManager PeerManager
	// taskTTL is task TTL
	taskTTL time.Duration
	// taskTTI is task TTI
	taskTTI time.Duration
	// tasks is task map
	tasks *sync.Map
}

func NewTaskManager(cfg *config.GCConfig, gcManager gc.GC, peerManager PeerManager) (TaskManager, error) {
	m := &taskManager{
		peerManager: peerManager,
		taskTTL:     cfg.TaskTTL,
		taskTTI:     cfg.TaskTTI,
		tasks:       &sync.Map{},
	}

	if err := gcManager.Add(gc.Task{
		ID:       TaskGCID,
		Interval: cfg.PeerGCInterval,
		Timeout:  cfg.PeerGCInterval,
		Runner:   m,
	}); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *taskManager) Delete(id string) {
	m.tasks.Delete(id)
}

func (m *taskManager) Add(task *Task) {
	m.tasks.Store(task.ID, task)
}

func (m *taskManager) Get(id string) (*Task, bool) {
	task, ok := m.tasks.Load(id)
	if !ok {
		return nil, false
	}

	return task.(*Task), ok
}

func (m *taskManager) GetOrAdd(t *Task) (*Task, bool) {
	task, ok := m.tasks.LoadOrStore(t.ID, t)
	return task.(*Task), ok
}

func (m *taskManager) RunGC() error {
	m.tasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		task := value.(*Task)
		elapsed := time.Since(task.lastAccessAt.Load())
		if elapsed > m.taskTTI && task.IsSuccess() {
			task.Log().Info("elapsed larger than taskTTI, task status become zombie")
			task.SetStatus(TaskStatusZombie)
		}

		if task.GetPeers().Len() == 0 {
			task.Log().Info("peers is empty, task status become waiting")
			task.SetStatus(TaskStatusWaiting)
		}

		if elapsed > m.taskTTL {
			// TODO lock
			peers := m.peerManager.GetPeersByTask(taskID)
			for _, peer := range peers {
				task.Log().Infof("delete peer %s because task is time to leave", peer.ID)
				m.peerManager.Delete(peer.ID)
			}
			task.Log().Info("delete task because elapsed larger than task TTL")
			m.Delete(taskID)
		}
		return true
	})
	return nil
}

type TaskStatus uint8

func (status TaskStatus) String() string {
	switch status {
	case TaskStatusWaiting:
		return "Waiting"
	case TaskStatusRunning:
		return "Running"
	case TaskStatusSeeding:
		return "Seeding"
	case TaskStatusSuccess:
		return "Success"
	case TaskStatusZombie:
		return "Zombie"
	case TaskStatusFail:
		return "Fail"
	default:
		return "unknown"
	}
}

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusSeeding
	TaskStatusSuccess
	TaskStatusZombie
	TaskStatusFail
)

type Task struct {
	// ID is task id
	ID string
	// URL is task download url
	URL string
	// URLMeta is task download url meta
	URLMeta *base.UrlMeta
	// DirectPiece is tiny piece data
	DirectPiece []byte
	// ContentLength is task total content length
	ContentLength atomic.Int64
	// CreateAt is peer create time
	CreateAt *atomic.Time
	// LastTriggerAt is peer last trigger time
	LastTriggerAt *atomic.Time
	// lastAccessAt is peer last access time
	lastAccessAt *atomic.Time
	// status is task status and type is TaskStatus
	status atomic.Value
	// peers is peer sorted unique list
	peers list.SortedUniqueList
	// BackToSourceWeight is back-to-source peer weight
	BackToSourceWeight atomic.Int32
	// backToSourcePeers is back-to-source peers list
	backToSourcePeers []string
	// pieces is piece map
	pieces *sync.Map
	// TotalPieceCount is total piece count
	TotalPieceCount atomic.Int32
	// task logger
	logger *logger.SugaredLoggerOnWith
	// task lock
	lock sync.RWMutex
}

func NewTask(id, url string, backToSourceWeight int32, meta *base.UrlMeta) *Task {
	now := time.Now()
	task := &Task{
		ID:                id,
		URL:               url,
		URLMeta:           meta,
		CreateAt:          atomic.NewTime(now),
		LastTriggerAt:     atomic.NewTime(now),
		lastAccessAt:      atomic.NewTime(now),
		backToSourcePeers: []string{},
		pieces:            &sync.Map{},
		peers:             list.NewSortedUniqueList(),
		logger:            logger.WithTaskID(id),
	}

	task.BackToSourceWeight.Store(backToSourceWeight)
	task.status.Store(TaskStatusWaiting)
	return task
}

func (task *Task) SetStatus(status TaskStatus) {
	task.status.Store(status)
}

func (task *Task) GetStatus() TaskStatus {
	return task.status.Load().(TaskStatus)
}

// IsSuccess determines that whether cdn status is success.
func (task *Task) IsSuccess() bool {
	return task.GetStatus() == TaskStatusSuccess
}

// CanSchedule determines whether task can be scheduled
// only task status is seeding or success can be scheduled
func (task *Task) CanSchedule() bool {
	return task.GetStatus() == TaskStatusSeeding || task.GetStatus() == TaskStatusSuccess
}

// IsWaiting determines whether task is waiting
func (task *Task) IsWaiting() bool {
	return task.GetStatus() == TaskStatusWaiting
}

// IsHealth determines whether task is health
func (task *Task) IsHealth() bool {
	return task.GetStatus() == TaskStatusRunning || task.GetStatus() == TaskStatusSeeding || task.GetStatus() == TaskStatusSuccess
}

// IsFail determines whether task is fail
func (task *Task) IsFail() bool {
	return task.GetStatus() == TaskStatusFail
}

func (task *Task) Touch() {
	task.lastAccessAt.Store(time.Now())
}

func (task *Task) UpdateSuccess(pieceCount int32, contentLength int64) {
	task.lock.Lock()
	defer task.lock.Unlock()

	if task.GetStatus() != TaskStatusSuccess {
		task.SetStatus(TaskStatusSuccess)
		task.TotalPieceCount.Store(pieceCount)
		task.ContentLength.Store(contentLength)
	}
}

func (task *Task) AddPeer(peer *Peer) {
	task.peers.Insert(peer)
}

func (task *Task) UpdatePeer(peer *Peer) {
	task.peers.Insert(peer)
}

func (task *Task) DeletePeer(peer *Peer) {
	task.peers.Remove(peer)
}

func (task *Task) GetPeers() list.SortedUniqueList {
	return task.peers
}

func (task *Task) GetPiece(n int32) (*base.PieceInfo, bool) {
	piece, ok := task.pieces.Load(n)
	if !ok {
		return nil, false
	}

	return piece.(*base.PieceInfo), ok
}

func (task *Task) GetOrAddPiece(p *base.PieceInfo) (*base.PieceInfo, bool) {
	piece, ok := task.pieces.LoadOrStore(p.PieceNum, p)
	return piece.(*base.PieceInfo), ok
}

func (task *Task) GetSizeScope() base.SizeScope {
	if task.ContentLength.Load() <= TinyFileSize {
		return base.SizeScope_TINY
	}

	if task.TotalPieceCount.Load() == 1 {
		return base.SizeScope_SMALL
	}

	return base.SizeScope_NORMAL
}

func (task *Task) CanBackToSource() bool {
	return task.BackToSourceWeight.Load() > 0
}

func (task *Task) ContainsBackToSourcePeer(peerID string) bool {
	task.lock.RLock()
	defer task.lock.RUnlock()

	for _, backToSourcePeer := range task.backToSourcePeers {
		if backToSourcePeer == peerID {
			return true
		}
	}
	return false
}

func (task *Task) AddBackToSourcePeer(peerID string) {
	if ok := task.ContainsBackToSourcePeer(peerID); ok {
		return
	}

	if task.BackToSourceWeight.Load() <= 0 {
		return
	}

	task.lock.Lock()
	defer task.lock.Unlock()

	task.backToSourcePeers = append(task.backToSourcePeers, peerID)
	task.BackToSourceWeight.Dec()
}

func (task *Task) GetBackToSourcePeers() []string {
	task.lock.RLock()
	defer task.lock.RUnlock()

	return task.backToSourcePeers
}

func (task *Task) Pick(limit int, pickFn func(peer *Peer) bool) []*Peer {
	var peers []*Peer

	task.GetPeers().Range(func(item list.Item) bool {
		if len(peers) >= limit {
			return false
		}
		peer, ok := item.(*Peer)
		if !ok {
			return true
		}

		if pickFn(peer) {
			peers = append(peers, peer)
		}
		return true
	})

	return peers
}

func (task *Task) PickReverse(limit int, pickFn func(peer *Peer) bool) []*Peer {
	var peers []*Peer

	task.GetPeers().ReverseRange(func(item list.Item) bool {
		if len(peers) >= limit {
			return false
		}
		peer, ok := item.(*Peer)
		if !ok {
			return true
		}

		if pickFn(peer) {
			peers = append(peers, peer)
		}
		return true
	})

	return peers
}

func (task *Task) Log() *logger.SugaredLoggerOnWith {
	return task.logger
}
