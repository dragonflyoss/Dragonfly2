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

package entity

import (
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/looplab/fsm"
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

const (
	// Peer has been created but did not start running
	PeerStatePending = "Pending"

	// Peer is downloading resources from CDN or back-to-source
	PeerStateRunning = "Running"

	// Peer is downloading resources finished
	PeerStateFinished = "Finished"

	// Peer has been downloaded successfully
	PeerStateSucceeded = "Succeeded"

	// Peer has been downloaded failed
	PeerStateFailed = "Failed"

	// Peer has been left
	PeerStateLeave = "Leave"
)

const (
	// Peer is downloading
	PeerEventDownload = "Download"

	// Peer downloaded finished
	PeerEventFinished = "Finished"

	// Peer downloaded successfully
	PeerEventSucceeded = "Succeeded"

	// Peer downloaded failed
	PeerEventFailed = "Failed"

	// Peer leaves
	PeerEventLeave = "Leave"
)

type Peer struct {
	// ID is peer id
	ID string

	// Pieces is piece bitset
	Pieces *bitset.BitSet

	// PieceCosts is piece downloaded time
	PieceCosts set.SafeSet

	// Stream is grpc stream instance
	Stream *atomic.Value

	// Record peer report piece grpc interface stop code
	StopChannel chan base.Code

	// Task state machine
	FSM *fsm.FSM

	// Task is peer task
	Task *Task

	// Host is peer host
	Host *Host

	// Parent is peer parent
	Parent *atomic.Value

	// Children is peer children
	Children *sync.Map

	// CreateAt is peer create time
	CreateAt *atomic.Time

	// UpdateAt is peer update time
	UpdateAt *atomic.Time

	// Peer mutex
	mu *sync.Mutex

	// Peer log
	Log *logger.SugaredLoggerOnWith
}

// New Peer instance
func NewPeer(id string, task *Task, host *Host) *Peer {
	p := &Peer{
		ID:          id,
		Pieces:      &bitset.BitSet{},
		PieceCosts:  set.NewSafeSet(),
		Stream:      &atomic.Value{},
		StopChannel: make(chan base.Code),
		Task:        task,
		Host:        host,
		Parent:      &atomic.Value{},
		Children:    &sync.Map{},
		CreateAt:    atomic.NewTime(time.Now()),
		UpdateAt:    atomic.NewTime(time.Now()),
		mu:          &sync.Mutex{},
		Log:         logger.WithTaskAndPeerID(task.ID, id),
	}

	// Initialize state machine
	p.FSM = fsm.NewFSM(
		PeerStatePending,
		fsm.Events{
			{Name: PeerEventDownload, Src: []string{PeerStatePending}, Dst: PeerStateRunning},
			{Name: PeerEventFinished, Src: []string{PeerStateRunning}, Dst: PeerEventFinished},
			{Name: PeerEventSucceeded, Src: []string{PeerEventFinished}, Dst: PeerStateSucceeded},
			{Name: PeerEventFailed, Src: []string{PeerStatePending, PeerStateRunning, PeerEventFinished}, Dst: PeerStateFailed},
			{Name: PeerEventLeave, Src: []string{PeerEventFailed, PeerStateSucceeded}, Dst: PeerEventLeave},
		},
		fsm.Callbacks{
			PeerEventDownload: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
			},
			PeerEventFinished: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
			},
			PeerEventSucceeded: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
			},
			PeerEventFailed: func(e *fsm.Event) {
				p.UpdateAt.Store(time.Now())
			},
		},
	)

	return p
}

// LoadChild return peer child for a key
func (p *Peer) LoadChild(key string) (*Peer, bool) {
	rawChild, ok := p.Children.Load(key)
	if !ok {
		return nil, false
	}

	return rawChild.(*Peer), ok
}

// StoreChild set peer child
func (p *Peer) StoreChild(child *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Children.Store(child.ID, child)
	p.Host.Peers.Store(child.ID, child)
	p.Task.Peers.Store(child.ID, child)
	child.Parent.Store(p)
}

// DeleteChild deletes peer child for a key
func (p *Peer) DeleteChild(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	child, ok := p.LoadChild(key)
	if !ok {
		return
	}

	p.Children.Delete(child.ID)
	p.Host.DeletePeer(child.ID)
	p.Task.DeletePeer(child.ID)
	child.Parent = &atomic.Value{}
}

// LoadParent return peer parent
func (p *Peer) LoadParent() (*Peer, bool) {
	rawParent := p.Parent.Load()
	if rawParent == nil {
		return nil, false
	}

	return rawParent.(*Peer), true
}

// StoreParent set peer parent
func (p *Peer) StoreParent(parent *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Parent.Store(parent)
	parent.Children.Store(p.ID, p)
	parent.Host.Peers.Store(p.ID, p)
	parent.Task.Peers.Store(p.ID, p)
}

// DeleteParent deletes peer parent
func (p *Peer) DeleteParent() {
	p.mu.Lock()
	defer p.mu.Unlock()

	parent, ok := p.LoadParent()
	if !ok {
		return
	}

	p.Parent = &atomic.Value{}
	parent.Children.Delete(p.ID)
	parent.Host.Peers.Delete(p.ID)
	parent.Task.Peers.Delete(p.ID)
}

// ReplaceParent replaces peer parent
func (p *Peer) ReplaceParent(parent *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.DeleteParent()
	p.StoreParent(parent)
}

// TreeTotalNodeCount represents tree's total node count
func (p *Peer) TreeTotalNodeCount() int {
	count := 1
	p.Children.Range(func(_, value interface{}) bool {
		node, ok := value.(*Peer)
		if !ok {
			return true
		}

		count += node.TreeTotalNodeCount()
		return true
	})

	return count
}

// IsDescendant determines whether it is ancestor of peer
func (p *Peer) IsDescendant(ancestor *Peer) bool {
	return isDescendant(ancestor, p)
}

// IsAncestor determines whether it is descendant of peer
func (p *Peer) IsAncestor(descendant *Peer) bool {
	return isDescendant(p, descendant)
}

// isDescendant determines whether it is ancestor of peer
func isDescendant(ancestor, descendant *Peer) bool {
	node := descendant
	for node != nil {
		parent, ok := node.LoadParent()
		if !ok {
			return false
		}
		if parent.ID == ancestor.ID {
			return true
		}
		node = parent
	}

	return false
}

// StoreStream set grpc stream
func (p *Peer) StoreStream(stream scheduler.Scheduler_ReportPieceResultServer) {
	p.Stream.Store(stream)
}

// LoadStream return grpc stream
func (p *Peer) LoadStream() (scheduler.Scheduler_ReportPieceResultServer, bool) {
	rawStream := p.Stream.Load()
	if rawStream == nil {
		return nil, false
	}

	return rawStream.(scheduler.Scheduler_ReportPieceResultServer), true
}

// DeleteStream deletes grpc stream
func (p *Peer) DeleteStream() {
	p.Stream = &atomic.Value{}
}
