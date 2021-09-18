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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	gc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/config"
	"go.uber.org/atomic"
)

const (
	TaskGCID     = "task"
	TinyFileSize = 128
)

type TaskManager interface {
	Add(*Task)

	Get(string) (*Task, bool)

	Delete(string)

	GetOrAdd(*Task) (*Task, bool)
}

type taskManager struct {
	peerManager PeerManager
	taskTTL     time.Duration
	taskTTI     time.Duration
	tasks       *sync.Map
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

		if task.GetPeers().Size() == 0 {
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
	lock          sync.RWMutex
	ID            string
	URL           string
	URLMeta       *base.UrlMeta
	DirectPiece   []byte
	CreateAt      *atomic.Time
	LastTriggerAt *atomic.Time
	lastAccessAt  *atomic.Time
	pieces        *sync.Map
	PieceTotal    atomic.Int32
	ContentLength atomic.Int64
	// status is task status and type is TaskStatus
	status           atomic.Value
	peers            *sortedlist.SortedList
	backSourceWeight atomic.Int32
	backSourcePeers  []string
	logger           *logger.SugaredLoggerOnWith
}

func NewTask(id, url string, meta *base.UrlMeta) *Task {
	task := &Task{
		ID:            id,
		URL:           url,
		URLMeta:       meta,
		CreateAt:      atomic.NewTime(time.Now()),
		LastTriggerAt: atomic.NewTime(time.Now()),
		lastAccessAt:  atomic.NewTime(time.Now()),
		pieces:        &sync.Map{},
		peers:         sortedlist.NewSortedList(),
		logger:        logger.WithTaskID(id),
	}

	task.status.Store(TaskStatusWaiting)
	return task
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

func (task *Task) GetPeers() *sortedlist.SortedList {
	return task.peers
}

func (task *Task) SetStatus(status TaskStatus) {
	task.status.Store(status)
}

func (task *Task) GetStatus() TaskStatus {
	return task.status.Load().(TaskStatus)
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

func (task *Task) Touch() {
	task.lastAccessAt.Store(time.Now())
}

func (task *Task) UpdateTaskSuccessResult(pieceTotal int32, contentLength int64) {
	task.lock.Lock()
	defer task.lock.Unlock()

	if task.GetStatus() != TaskStatusSuccess {
		task.SetStatus(TaskStatusSuccess)
		task.PieceTotal.Store(pieceTotal)
		task.ContentLength.Store(contentLength)
	}
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

func (task *Task) SetClientBackSource(backSourceLimit int32) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.backSourcePeers = make([]string, 0, backSourceLimit)
	task.backSourceWeight.Store(backSourceLimit)
}

func (task *Task) NeedClientBackSource() bool {
	return task.backSourceWeight.Load() > 0
}

func (task *Task) ContainsBackSourcePeer(peerID string) bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	for _, backSourcePeer := range task.backSourcePeers {
		if backSourcePeer == peerID {
			return true
		}
	}
	return false
}

func (task *Task) AddBackSourcePeer(peerID string) {
	if ok := task.ContainsBackSourcePeer(peerID); ok {
		return
	}

	task.lock.Lock()
	defer task.lock.Unlock()
	task.backSourcePeers = append(task.backSourcePeers, peerID)
	task.backSourceWeight.Dec()
}

func (task *Task) GetBackSourcePeers() []string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.backSourcePeers
}

func (task *Task) Pick(limit int, pickFn func(peer *Peer) bool) (pickedPeers []*Peer) {
	return task.pick(limit, false, pickFn)
}

func (task *Task) PickReverse(limit int, pickFn func(peer *Peer) bool) (pickedPeers []*Peer) {
	return task.pick(limit, true, pickFn)
}

func (task *Task) pick(limit int, reverse bool, pickFn func(peer *Peer) bool) (pickedPeers []*Peer) {
	if pickFn == nil {
		return
	}

	if !reverse {
		task.GetPeers().Range(func(data sortedlist.Item) bool {
			if len(pickedPeers) >= limit {
				return false
			}
			peer := data.(*Peer)
			if pickFn(peer) {
				pickedPeers = append(pickedPeers, peer)
			}
			return true
		})
		return
	}

	task.GetPeers().RangeReverse(func(data sortedlist.Item) bool {
		if len(pickedPeers) >= limit {
			return false
		}
		peer := data.(*Peer)
		if pickFn(peer) {
			pickedPeers = append(pickedPeers, peer)
		}
		return true
	})

	return
}

func (task *Task) Log() *logger.SugaredLoggerOnWith {
	return task.logger
}
