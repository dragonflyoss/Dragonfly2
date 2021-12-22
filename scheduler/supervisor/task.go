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
	"d7y.io/dragonfly/v2/pkg/container/set"
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
	// tasks is task map
	tasks *sync.Map
}

func NewTaskManager(cfg *config.GCConfig, gcManager gc.GC, peerManager PeerManager) (TaskManager, error) {
	m := &taskManager{
		peerManager: peerManager,
		taskTTL:     cfg.TaskTTL,
		tasks:       &sync.Map{},
	}

	if err := gcManager.Add(gc.Task{
		ID:       TaskGCID,
		Interval: cfg.TaskGCInterval,
		Timeout:  cfg.TaskGCInterval,
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
		elapsed := time.Since(task.LastAccessAt.Load())

		if task.GetPeers().Len() == 0 && elapsed > m.taskTTL {
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
	case TaskStatusSuccess:
		return "Success"
	case TaskStatusFail:
		return "Fail"
	default:
		return "unknown"
	}
}

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusSuccess
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
	// LastAccessAt is peer last access time
	LastAccessAt *atomic.Time
	// status is task status and type is TaskStatus
	status atomic.Value
	// peers is peer safe set
	peers set.SafeSet
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
		LastAccessAt:      atomic.NewTime(now),
		backToSourcePeers: []string{},
		pieces:            &sync.Map{},
		peers:             set.NewSafeSet(),
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

// IsWaiting determines whether task is waiting
func (task *Task) IsWaiting() bool {
	return task.GetStatus() == TaskStatusWaiting
}

// IsHealth determines whether task is health
func (task *Task) IsHealth() bool {
	return task.GetStatus() == TaskStatusRunning || task.GetStatus() == TaskStatusSuccess
}

// IsFail determines whether task is fail
func (task *Task) IsFail() bool {
	return task.GetStatus() == TaskStatusFail
}

func (task *Task) AddPeer(peer *Peer) {
	task.peers.Add(peer)
}

func (task *Task) DeletePeer(peer *Peer) {
	task.peers.Delete(peer)
}

func (task *Task) GetPeers() set.SafeSet {
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

func (task *Task) Log() *logger.SugaredLoggerOnWith {
	return task.logger
}
