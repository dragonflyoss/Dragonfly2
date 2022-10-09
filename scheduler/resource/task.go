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

package resource

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/dag"
)

const (
	// Tiny file size is 128 bytes.
	TinyFileSize = 128

	// Empty file size is 0 bytes.
	EmptyFileSize = 0
)

const (
	// Peer failure limit in task.
	FailedPeerCountLimit = 200

	// Peer count limit for task.
	PeerCountLimitForTask = 10 * 1000
)

const (
	// Task has been created but did not start running.
	TaskStatePending = "Pending"

	// Task is downloading resources from seed peer or back-to-source.
	TaskStateRunning = "Running"

	// Task has been downloaded successfully.
	TaskStateSucceeded = "Succeeded"

	// Task has been downloaded failed.
	TaskStateFailed = "Failed"

	// Task has no peers.
	TaskStateLeave = "Leave"
)

const (
	// Task is downloading.
	TaskEventDownload = "Download"

	// Task downloaded successfully.
	TaskEventDownloadSucceeded = "DownloadSucceeded"

	// Task downloaded failed.
	TaskEventDownloadFailed = "DownloadFailed"

	// Task leaves.
	TaskEventLeave = "Leave"
)

// Option is a functional option for task.
type Option func(task *Task)

// WithBackToSourceLimit set BackToSourceLimit for task.
func WithBackToSourceLimit(limit int32) Option {
	return func(task *Task) {
		task.BackToSourceLimit.Add(limit)
	}
}

type Task struct {
	// ID is task id.
	ID string

	// URL is task download url.
	URL string

	// Type is task type.
	Type commonv1.TaskType

	// URLMeta is task download url meta.
	URLMeta *commonv1.UrlMeta

	// DirectPiece is tiny piece data.
	DirectPiece []byte

	// ContentLength is task total content length.
	ContentLength *atomic.Int64

	// TotalPieceCount is total piece count.
	TotalPieceCount *atomic.Int32

	// BackToSourceLimit is back-to-source limit.
	BackToSourceLimit *atomic.Int32

	// BackToSourcePeers is back-to-source sync map.
	BackToSourcePeers set.SafeSet[string]

	// Task state machine.
	FSM *fsm.FSM

	// Piece sync map.
	Pieces *sync.Map

	// DAG is directed acyclic graph of peers.
	DAG dag.DAG[*Peer]

	// PeerFailedCount is peer failed count,
	// if one peer succeeds, the value is reset to zero.
	PeerFailedCount *atomic.Int32

	// CreateAt is task create time.
	CreateAt *atomic.Time

	// UpdateAt is task update time.
	UpdateAt *atomic.Time

	// Task log.
	Log *logger.SugaredLoggerOnWith
}

// New task instance.
func NewTask(id, url string, taskType commonv1.TaskType, meta *commonv1.UrlMeta, options ...Option) *Task {
	t := &Task{
		ID:                id,
		URL:               url,
		Type:              taskType,
		URLMeta:           meta,
		ContentLength:     atomic.NewInt64(0),
		TotalPieceCount:   atomic.NewInt32(0),
		BackToSourceLimit: atomic.NewInt32(0),
		BackToSourcePeers: set.NewSafeSet[string](),
		Pieces:            &sync.Map{},
		DAG:               dag.NewDAG[*Peer](),
		PeerFailedCount:   atomic.NewInt32(0),
		CreateAt:          atomic.NewTime(time.Now()),
		UpdateAt:          atomic.NewTime(time.Now()),
		Log:               logger.WithTaskIDAndURL(id, url),
	}

	// Initialize state machine.
	t.FSM = fsm.NewFSM(
		TaskStatePending,
		fsm.Events{
			{Name: TaskEventDownload, Src: []string{TaskStateLeave, TaskStatePending, TaskStateSucceeded, TaskStateFailed}, Dst: TaskStateRunning},
			{Name: TaskEventDownloadSucceeded, Src: []string{TaskStateLeave, TaskStateRunning, TaskStateFailed}, Dst: TaskStateSucceeded},
			{Name: TaskEventDownloadFailed, Src: []string{TaskStateRunning}, Dst: TaskStateFailed},
			{Name: TaskEventLeave, Src: []string{TaskStatePending, TaskStateRunning, TaskStateSucceeded, TaskStateFailed}, Dst: TaskStateLeave},
		},
		fsm.Callbacks{
			TaskEventDownload: func(e *fsm.Event) {
				t.UpdateAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventDownloadSucceeded: func(e *fsm.Event) {
				t.UpdateAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventDownloadFailed: func(e *fsm.Event) {
				t.UpdateAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventLeave: func(e *fsm.Event) {
				t.UpdateAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
		},
	)

	for _, opt := range options {
		opt(t)
	}

	return t
}

// LoadPeer return peer for a key.
func (t *Task) LoadPeer(key string) (*Peer, bool) {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		t.Log.Error(err)
		return nil, false
	}

	return vertex.Value, true
}

// LoadRandomPeers return random peers.
func (t *Task) LoadRandomPeers(n uint) []*Peer {
	var peers []*Peer
	for _, vertex := range t.DAG.GetRandomVertices(n) {
		peers = append(peers, vertex.Value)
	}

	return peers
}

// StorePeer set peer.
func (t *Task) StorePeer(peer *Peer) {
	t.DAG.AddVertex(peer.ID, peer) // nolint: errcheck
}

// DeletePeer deletes peer for a key.
func (t *Task) DeletePeer(key string) {
	if err := t.DeletePeerInEdges(key); err != nil {
		t.Log.Error(err)
	}

	if err := t.DeletePeerOutEdges(key); err != nil {
		t.Log.Error(err)
	}

	t.DAG.DeleteVertex(key)
}

// PeerCount returns count of peer.
func (t *Task) PeerCount() int {
	return t.DAG.VertexCount()
}

// AddPeerEdge adds inedges between two peers.
func (t *Task) AddPeerEdge(fromPeer *Peer, toPeer *Peer) error {
	if err := t.DAG.AddEdge(fromPeer.ID, toPeer.ID); err != nil {
		return err
	}

	fromPeer.Host.UploadPeerCount.Inc()
	return nil
}

// DeletePeerInEdges deletes inedges of peer.
func (t *Task) DeletePeerInEdges(key string) error {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		return err
	}

	for _, parent := range vertex.Parents.Values() {
		if parent.Value == nil {
			continue
		}

		parent.Value.Host.UploadPeerCount.Dec()
	}

	vertex.DeleteInEdges()
	return nil
}

// DeletePeerOutEdges deletes outedges of peer.
func (t *Task) DeletePeerOutEdges(key string) error {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		return err
	}

	peer := vertex.Value
	if peer == nil {
		return errors.New("vertex value is nil")
	}

	peer.Host.UploadPeerCount.Sub(int32(vertex.Children.Len()))
	vertex.DeleteOutEdges()
	return nil
}

// CanAddPeerEdge finds whether there are peer circles through depth-first search.
func (t *Task) CanAddPeerEdge(fromPeerKey, toPeerKey string) bool {
	return t.DAG.CanAddEdge(fromPeerKey, toPeerKey)
}

// PeerDegree returns the degree of peer.
func (t *Task) PeerDegree(key string) (int, error) {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		return 0, err
	}

	return vertex.Degree(), nil
}

// PeerInDegree returns the indegree of peer.
func (t *Task) PeerInDegree(key string) (int, error) {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		return 0, err
	}

	return vertex.InDegree(), nil
}

// PeerOutDegree returns the outdegree of peer.
func (t *Task) PeerOutDegree(key string) (int, error) {
	vertex, err := t.DAG.GetVertex(key)
	if err != nil {
		return 0, err
	}

	return vertex.OutDegree(), nil
}

// HasAvailablePeer returns whether there is an available peer.
func (t *Task) HasAvailablePeer() bool {
	var hasAvailablePeer bool
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if peer.FSM.Is(PeerStateSucceeded) {
			hasAvailablePeer = true
			break
		}
	}

	return hasAvailablePeer
}

// LoadSeedPeer return latest seed peer in peers sync map.
func (t *Task) LoadSeedPeer() (*Peer, bool) {
	var peers []*Peer
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if peer.Host.Type != HostTypeNormal {
			peers = append(peers, peer)
		}
	}

	sort.Slice(
		peers,
		func(i, j int) bool {
			return peers[i].UpdateAt.Load().After(peers[j].UpdateAt.Load())
		},
	)

	if len(peers) > 0 {
		return peers[0], true
	}

	return nil, false
}

// IsSeedPeerFailed returns whether the seed peer in the task failed.
func (t *Task) IsSeedPeerFailed() bool {
	seedPeer, ok := t.LoadSeedPeer()
	return ok && seedPeer.FSM.Is(PeerStateFailed) && time.Since(seedPeer.CreateAt.Load()) < SeedPeerFailedTimeout
}

// LoadPiece return piece for a key.
func (t *Task) LoadPiece(key int32) (*commonv1.PieceInfo, bool) {
	rawPiece, ok := t.Pieces.Load(key)
	if !ok {
		return nil, false
	}

	return rawPiece.(*commonv1.PieceInfo), ok
}

// StorePiece set piece.
func (t *Task) StorePiece(piece *commonv1.PieceInfo) {
	t.Pieces.Store(piece.PieceNum, piece)
}

// DeletePiece deletes piece for a key.
func (t *Task) DeletePiece(key int32) {
	t.Pieces.Delete(key)
}

// SizeScope return task size scope type.
func (t *Task) SizeScope() (commonv1.SizeScope, error) {
	if t.ContentLength.Load() < 0 {
		return -1, errors.New("invalid content length")
	}

	if t.TotalPieceCount.Load() < 0 {
		return -1, errors.New("invalid total piece count")
	}

	if t.ContentLength.Load() == EmptyFileSize {
		return commonv1.SizeScope_EMPTY, nil
	}

	if t.ContentLength.Load() <= TinyFileSize {
		return commonv1.SizeScope_TINY, nil
	}

	if t.TotalPieceCount.Load() == 1 {
		return commonv1.SizeScope_SMALL, nil
	}

	return commonv1.SizeScope_NORMAL, nil
}

// CanBackToSource represents whether peer can back-to-source.
func (t *Task) CanBackToSource() bool {
	return int32(t.BackToSourcePeers.Len()) <= t.BackToSourceLimit.Load() && (t.Type == commonv1.TaskType_Normal || t.Type == commonv1.TaskType_DfStore)
}

// NotifyPeers notify all peers in the task with the state code.
func (t *Task) NotifyPeers(peerPacket *schedulerv1.PeerPacket, event string) {
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if peer.FSM.Is(PeerStateRunning) {
			stream, ok := peer.LoadStream()
			if !ok {
				continue
			}

			if err := stream.Send(peerPacket); err != nil {
				t.Log.Errorf("send packet to peer %s failed: %s", peer.ID, err.Error())
				continue
			}
			t.Log.Infof("task notify peer %s code %s", peer.ID, peerPacket.Code)

			if err := peer.FSM.Event(event); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				continue
			}
		}
	}
}
