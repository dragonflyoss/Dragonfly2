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
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type PeerStatus uint8

func (status PeerStatus) String() string {
	switch status {
	case PeerStatusWaiting:
		return "Waiting"
	case PeerStatusRunning:
		return "Running"
	case PeerStatusFail:
		return "Fail"
	case PeerStatusSuccess:
		return "Success"
	default:
		return "unknown"
	}
}

const (
	PeerStatusWaiting PeerStatus = iota
	PeerStatusRunning
	PeerStatusFail
	PeerStatusSuccess
)

type Peer struct {
	// ID is ID of peer
	ID string
	// Task is peer task
	Task *Task
	// Host is peer host
	Host *Host
	// CreateAt is peer create time
	CreateAt *atomic.Time
	// LastAccessAt is peer last access time
	LastAccessAt *atomic.Time
	// parent is peer parent and type is *Peer
	parent atomic.Value
	// children is peer children map
	children *sync.Map
	// status is peer status and type is PeerStatus
	status atomic.Value
	// pieces is piece bitset
	Pieces bitset.BitSet
	// pieceCosts is piece historical download time
	pieceCosts []int
	// stream is grpc stream instance
	stream atomic.Value
	// leave is whether the peer leaves
	leave atomic.Bool
	// peer logger
	logger *logger.SugaredLoggerOnWith
	// peer lock
	lock sync.RWMutex
}

func NewPeer(id string, task *Task, host *Host) *Peer {
	peer := &Peer{
		ID:           id,
		Task:         task,
		Host:         host,
		Pieces:       bitset.BitSet{},
		CreateAt:     atomic.NewTime(time.Now()),
		LastAccessAt: atomic.NewTime(time.Now()),
		children:     &sync.Map{},
		logger:       logger.WithTaskAndPeerID(task.ID, id),
	}

	peer.status.Store(PeerStatusWaiting)
	return peer
}

func (peer *Peer) GetTreeNodeCount() int {
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		node := value.(*Peer)
		count += node.GetTreeNodeCount()
		return true
	})

	return count
}

func (peer *Peer) GetTreeDepth() int {
	var deep int
	node := peer
	for node != nil {
		deep++
		parent, ok := node.GetParent()
		if !ok || node.Host.IsCDN {
			break
		}
		node = parent
	}
	return deep
}

func (peer *Peer) GetRoot() *Peer {
	node := peer
	for node != nil {
		parent, ok := node.GetParent()
		if !ok || node.Host.IsCDN {
			break
		}
		node = parent
	}

	return node
}

// IsDescendant if peer is offspring of ancestor
func (peer *Peer) IsDescendant(ancestor *Peer) bool {
	return isDescendant(ancestor, peer)
}

func isDescendant(ancestor, offspring *Peer) bool {
	if ancestor == nil || offspring == nil {
		return false
	}
	// TODO avoid circulation
	node := offspring
	for node != nil {
		parent, ok := node.GetParent()
		if !ok {
			return false
		} else if parent.ID == ancestor.ID {
			return true
		}
		node = parent
	}
	return false
}

// IsAncestorOf if offspring is offspring of peer
func (peer *Peer) IsAncestor(offspring *Peer) bool {
	return isDescendant(peer, offspring)
}

func (peer *Peer) insertChild(child *Peer) {
	peer.children.Store(child.ID, child)
	peer.Host.CurrentUploadLoad.Inc()
	peer.Task.AddPeer(peer)
}

func (peer *Peer) deleteChild(child *Peer) {
	peer.children.Delete(child.ID)
	peer.Host.CurrentUploadLoad.Dec()
	peer.Task.AddPeer(peer)
}

func (peer *Peer) ReplaceParent(parent *Peer) {
	oldParent, ok := peer.GetParent()
	if ok {
		oldParent.deleteChild(peer)
	}

	peer.SetParent(parent)
	parent.insertChild(peer)
}

func (peer *Peer) DeleteParent() {
	oldParent, ok := peer.GetParent()
	if ok {
		oldParent.deleteChild(peer)
	}

	peer.SetParent(nil)
}

func (peer *Peer) GetChildren() *sync.Map {
	return peer.children
}

func (peer *Peer) SetParent(parent *Peer) {
	peer.parent.Store(parent)
}

func (peer *Peer) GetParent() (*Peer, bool) {
	parent := peer.parent.Load()
	if parent == nil {
		return nil, false
	}

	p, ok := parent.(*Peer)
	if p == nil || !ok {
		return nil, false
	}

	return p, true
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()

}

func (peer *Peer) GetPieceCosts() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()

	return peer.pieceCosts
}

func (peer *Peer) SetPieceCosts(costs ...int) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.pieceCosts = append(peer.pieceCosts, costs...)
}

func (peer *Peer) AddPiece(num int32, cost int) {
	peer.Pieces.Set(uint(num))
	peer.SetPieceCosts(cost)
	peer.Task.AddPeer(peer)
}

func (peer *Peer) getFreeLoad() int32 {
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
}

func (peer *Peer) SetStatus(status PeerStatus) {
	peer.status.Store(status)
}

func (peer *Peer) GetStatus() PeerStatus {
	return peer.status.Load().(PeerStatus)
}

func (peer *Peer) Leave() {
	peer.leave.Store(true)
}

func (peer *Peer) IsLeave() bool {
	return peer.leave.Load()
}

func (peer *Peer) IsRunning() bool {
	return peer.GetStatus() == PeerStatusRunning
}

func (peer *Peer) IsWaiting() bool {
	return peer.GetStatus() == PeerStatusWaiting
}

func (peer *Peer) IsSuccess() bool {
	return peer.GetStatus() == PeerStatusSuccess
}

func (peer *Peer) IsDone() bool {
	return peer.GetStatus() == PeerStatusSuccess || peer.GetStatus() == PeerStatusFail
}

func (peer *Peer) IsFail() bool {
	return peer.GetStatus() == PeerStatusFail
}

func (peer *Peer) SetStream(stream scheduler.Scheduler_ReportPieceResultServer) {
	peer.stream.Store(stream)
}

func (peer *Peer) GetStream() (scheduler.Scheduler_ReportPieceResultServer, bool) {
	rawStream := peer.stream.Load()
	stream, ok := rawStream.(scheduler.Scheduler_ReportPieceResultServer)
	if stream == nil || !ok {
		return nil, false
	}

	return stream, true
}

func (peer *Peer) Log() *logger.SugaredLoggerOnWith {
	return peer.logger
}
