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
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/graph/dag"
	"d7y.io/dragonfly/v2/pkg/types"
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
	PeerCountLimitForTask = 500
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

// TaskOption is a functional option for task.
type TaskOption func(task *Task)

// WithPieceLength set PieceLength for task.
func WithPieceLength(pieceLength int32) TaskOption {
	return func(t *Task) {
		t.PieceLength = pieceLength
	}
}

// WithDigest set Digest for task.
func WithDigest(d *digest.Digest) TaskOption {
	return func(t *Task) {
		t.Digest = d
	}
}

// Task contains content for task.
type Task struct {
	// ID is task id.
	ID string

	// Type is task type.
	Type commonv2.TaskType

	// URL is task download url.
	URL string

	// Digest of the task content, for example md5:xxx or sha256:yyy.
	Digest *digest.Digest

	// URL tag identifies different task for same url.
	Tag string

	// Application identifies different task for same url.
	Application string

	// Filter url used to generate task id.
	Filters []string

	// Task request headers.
	Header map[string]string

	// Task piece length.
	PieceLength int32

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

	// CreatedAt is task create time.
	CreatedAt *atomic.Time

	// UpdatedAt is task update time.
	UpdatedAt *atomic.Time

	// Task log.
	Log *logger.SugaredLoggerOnWith
}

// New task instance.
func NewTask(id, url, tag, application string, typ commonv2.TaskType, filters []string,
	header map[string]string, backToSourceLimit int32, options ...TaskOption) *Task {
	t := &Task{
		ID:                id,
		Type:              typ,
		URL:               url,
		Tag:               tag,
		Application:       application,
		Filters:           filters,
		Header:            header,
		DirectPiece:       []byte{},
		ContentLength:     atomic.NewInt64(-1),
		TotalPieceCount:   atomic.NewInt32(0),
		BackToSourceLimit: atomic.NewInt32(backToSourceLimit),
		BackToSourcePeers: set.NewSafeSet[string](),
		Pieces:            &sync.Map{},
		DAG:               dag.NewDAG[*Peer](),
		PeerFailedCount:   atomic.NewInt32(0),
		CreatedAt:         atomic.NewTime(time.Now()),
		UpdatedAt:         atomic.NewTime(time.Now()),
		Log:               logger.WithTask(id, url),
	}

	// Initialize state machine.
	t.FSM = fsm.NewFSM(
		TaskStatePending,
		fsm.Events{
			{Name: TaskEventDownload, Src: []string{TaskStatePending, TaskStateSucceeded, TaskStateFailed, TaskStateLeave}, Dst: TaskStateRunning},
			{Name: TaskEventDownloadSucceeded, Src: []string{TaskStateLeave, TaskStateRunning, TaskStateFailed}, Dst: TaskStateSucceeded},
			{Name: TaskEventDownloadFailed, Src: []string{TaskStateRunning}, Dst: TaskStateFailed},
			{Name: TaskEventLeave, Src: []string{TaskStatePending, TaskStateRunning, TaskStateSucceeded, TaskStateFailed}, Dst: TaskStateLeave},
		},
		fsm.Callbacks{
			TaskEventDownload: func(ctx context.Context, e *fsm.Event) {
				t.UpdatedAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventDownloadSucceeded: func(ctx context.Context, e *fsm.Event) {
				t.UpdatedAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventDownloadFailed: func(ctx context.Context, e *fsm.Event) {
				t.UpdatedAt.Store(time.Now())
				t.Log.Infof("task state is %s", e.FSM.Current())
			},
			TaskEventLeave: func(ctx context.Context, e *fsm.Event) {
				t.UpdatedAt.Store(time.Now())
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

	fromPeer.Host.ConcurrentUploadCount.Inc()
	fromPeer.Host.UploadCount.Inc()
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

		parent.Value.Host.ConcurrentUploadCount.Dec()
	}

	if err := t.DAG.DeleteVertexInEdges(key); err != nil {
		return err
	}

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
	peer.Host.ConcurrentUploadCount.Sub(int32(vertex.Children.Len()))

	if err := t.DAG.DeleteVertexOutEdges(key); err != nil {
		return err
	}

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
func (t *Task) HasAvailablePeer(blocklist set.SafeSet[string]) bool {
	var hasAvailablePeer bool
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if blocklist.Contains(peer.ID) {
			continue
		}

		if peer.FSM.Is(PeerStateRunning) ||
			peer.FSM.Is(PeerStateSucceeded) ||
			peer.FSM.Is(PeerStateBackToSource) {
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

		if peer.Host.Type != types.HostTypeNormal {
			peers = append(peers, peer)
		}
	}

	sort.Slice(
		peers,
		func(i, j int) bool {
			return peers[i].UpdatedAt.Load().After(peers[j].UpdatedAt.Load())
		},
	)

	if len(peers) > 0 {
		return peers[0], true
	}

	return nil, false
}

// IsSeedPeerFailed returns whether the seed peer in the task failed.
func (t *Task) IsSeedPeerFailed() bool {
	seedPeer, loaded := t.LoadSeedPeer()
	return loaded && seedPeer.FSM.Is(PeerStateFailed) && time.Since(seedPeer.CreatedAt.Load()) < SeedPeerFailedTimeout
}

// LoadPiece return piece for a key.
func (t *Task) LoadPiece(key int32) (*Piece, bool) {
	rawPiece, loaded := t.Pieces.Load(key)
	if !loaded {
		return nil, false
	}

	return rawPiece.(*Piece), loaded
}

// StorePiece set piece.
func (t *Task) StorePiece(piece *Piece) {
	t.Pieces.Store(piece.Number, piece)
}

// DeletePiece deletes piece for a key.
func (t *Task) DeletePiece(key int32) {
	t.Pieces.Delete(key)
}

// SizeScope return task size scope type.
func (t *Task) SizeScope() commonv2.SizeScope {
	if t.ContentLength.Load() < 0 {
		return commonv2.SizeScope_UNKNOW
	}

	if t.TotalPieceCount.Load() < 0 {
		return commonv2.SizeScope_UNKNOW
	}

	if t.ContentLength.Load() == EmptyFileSize {
		return commonv2.SizeScope_EMPTY
	}

	if t.ContentLength.Load() <= TinyFileSize {
		return commonv2.SizeScope_TINY
	}

	if t.TotalPieceCount.Load() == 1 {
		return commonv2.SizeScope_SMALL
	}

	return commonv2.SizeScope_NORMAL
}

// CanBackToSource represents whether task can back-to-source.
func (t *Task) CanBackToSource() bool {
	return int32(t.BackToSourcePeers.Len()) <= t.BackToSourceLimit.Load() && (t.Type == commonv2.TaskType_DFDAEMON || t.Type == commonv2.TaskType_DFSTORE)
}

// CanReuseDirectPiece represents whether task can reuse data of direct piece.
func (t *Task) CanReuseDirectPiece() bool {
	return len(t.DirectPiece) > 0 && int64(len(t.DirectPiece)) == t.ContentLength.Load()
}

// ReportPieceResultToPeers reports all peers in the task with the state code.
// Used only in v1 version of the grpc.
func (t *Task) ReportPieceResultToPeers(peerPacket *schedulerv1.PeerPacket, event string) {
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if peer.FSM.Is(PeerStateRunning) {
			stream, loaded := peer.LoadReportPieceResultStream()
			if !loaded {
				continue
			}

			if err := stream.Send(peerPacket); err != nil {
				t.Log.Errorf("send packet to peer %s failed: %s", peer.ID, err.Error())
				continue
			}
			t.Log.Infof("task reports peer %s code %s", peer.ID, peerPacket.Code)

			if err := peer.FSM.Event(context.Background(), event); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				continue
			}
		}
	}
}

// AnnouncePeers announces all peers in the task with the state code.
// Used only in v2 version of the grpc.
func (t *Task) AnnouncePeers(resp *schedulerv2.AnnouncePeerResponse, event string) {
	for _, vertex := range t.DAG.GetVertices() {
		peer := vertex.Value
		if peer == nil {
			continue
		}

		if peer.FSM.Is(PeerStateRunning) {
			stream, loaded := peer.LoadAnnouncePeerStream()
			if !loaded {
				continue
			}

			if err := stream.Send(resp); err != nil {
				t.Log.Errorf("send response to peer %s failed: %s", peer.ID, err.Error())
				continue
			}
			t.Log.Infof("task announces peer %s response %#v", peer.ID, resp.Response)

			if err := peer.FSM.Event(context.Background(), event); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				continue
			}
		}
	}
}
