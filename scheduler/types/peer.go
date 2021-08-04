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

	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"go.uber.org/atomic"
)

type PeerStatus uint8

func (status PeerStatus) String() string {
	switch status {
	case PeerStatusWaiting:
		return "Waiting"
	case PeerStatusRunning:
		return "Running"
	case PeerStatusSuccess:
		return "Success"
	case PeerStatusFail:
		return "fail"
	case PeerStatusZombie:
		return "zombie"
	default:
		return "unknown"
	}
}

const (
	PeerStatusWaiting PeerStatus = iota
	PeerStatusRunning
	PeerStatusZombie
	PeerStatusFail
	PeerStatusSuccess
)

type Peer struct {
	lock sync.RWMutex
	// PeerID specifies ID of peer
	PeerID string
	// Task specifies
	Task *Task
	// Host specifies
	Host *PeerHost
	// bindPacketChan
	bindPacketChan bool
	// PacketChan send schedulerPacket to peer client
	packetChan chan *scheduler.PeerPacket
	// createTime
	CreateTime time.Time
	// finishedNum specifies downloaded finished piece number
	finishedNum    atomic.Int32
	lastAccessTime time.Time
	parent         *Peer
	children       sync.Map
	status         PeerStatus
	costHistory    []int
	leave          atomic.Bool
}

func NewPeer(peerID string, task *Task, host *PeerHost) *Peer {
	return &Peer{
		PeerID:         peerID,
		Task:           task,
		Host:           host,
		CreateTime:     time.Now(),
		lastAccessTime: time.Now(),
		status:         PeerStatusWaiting,
	}
}

func (peer *Peer) GetWholeTreeNode() int {
	// TODO lock task
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	count := 1
	peer.children.Range(func(key, value interface{}) bool {
		peerNode := value.(*Peer)
		count += peerNode.GetWholeTreeNode()
		return true
	})
	return count
}

func (peer *Peer) GetLastAccessTime() time.Time {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.lastAccessTime
}

func (peer *Peer) Touch() {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.lastAccessTime = time.Now()
	if peer.status == PeerStatusZombie && !peer.leave.Load() {
		peer.status = PeerStatusRunning
	}
	peer.Task.Touch()
}

func (peer *Peer) associateChild(child *Peer) {
	peer.lock.Lock()
	peer.children.Store(child.PeerID, child)
	peer.Host.IncUploadLoad()
	peer.lock.Unlock()
	peer.Task.peers.Update(peer)
}

func (peer *Peer) disassociateChild(child *Peer) {
	peer.lock.Lock()
	peer.children.Delete(child.PeerID)
	peer.Host.DecUploadLoad()
	peer.lock.Unlock()
	peer.Task.peers.Update(peer)
}

func (peer *Peer) ReplaceParent(parent *Peer) {
	oldParent := peer.parent
	if oldParent != nil {
		oldParent.disassociateChild(peer)
	}
	peer.parent = parent
	if parent != nil {
		parent.associateChild(peer)
	}
}

func (peer *Peer) GetCostHistory() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.costHistory
}

func (peer *Peer) GetCost() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	if len(peer.costHistory) < 1 {
		return int(time.Second / time.Millisecond)
	}
	totalCost := 0
	for _, cost := range peer.costHistory {
		totalCost += cost
	}
	return totalCost / len(peer.costHistory)
}

func (peer *Peer) AddPieceInfo(finishedCount int32, cost int) {
	peer.lock.Lock()
	if finishedCount > peer.finishedNum.Load() {
		peer.finishedNum.Store(finishedCount)
		peer.costHistory = append(peer.costHistory, cost)
		if len(peer.costHistory) > 20 {
			peer.costHistory = peer.costHistory[len(peer.costHistory)-20:]
		}
		peer.lock.Unlock()
		peer.Task.peers.Update(peer)
		return
	}
	peer.lock.Unlock()
}

func (peer *Peer) GetDepth() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	var deep int
	node := peer
	for node != nil {
		deep++
		if node.parent == nil || node.Host.CDN {
			break
		}
		node = node.parent
	}
	return deep
}

func (peer *Peer) GetTreeRoot() *Peer {
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.CDN {
			break
		}
		node = node.parent
	}
	return node
}

// IsDescendantOf if peer is offspring of ancestor
func (peer *Peer) IsDescendantOf(ancestor *Peer) bool {
	if ancestor == nil {
		return false
	}
	// TODO avoid circulation
	peer.lock.RLock()
	ancestor.lock.RLock()
	defer ancestor.lock.RUnlock()
	defer peer.lock.RUnlock()
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.CDN {
			return false
		} else if node.PeerID == ancestor.PeerID {
			return true
		}
		node = node.parent
	}
	return false
}

// IsAncestorOf if offspring is offspring of peer
func (peer *Peer) IsAncestorOf(offspring *Peer) bool {
	if offspring == nil {
		return false
	}
	offspring.lock.RLock()
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	defer offspring.lock.RUnlock()
	node := offspring
	for node != nil {
		if node.parent == nil || node.Host.CDN {
			return false
		} else if node.PeerID == peer.PeerID {
			return true
		}
		node = node.parent
	}
	return false
}

func (peer *Peer) IsBlocking() bool {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	if peer.parent == nil {
		return false
	}
	return peer.finishedNum.Load() >= peer.parent.finishedNum.Load()
}

func (peer *Peer) GetSortKeys() (key1, key2 int) {
	key1 = int(peer.finishedNum.Load())
	key2 = peer.getFreeLoad()
	return
}

func (peer *Peer) getFreeLoad() int {
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
}

func GetDiffPieceNum(src *Peer, dst *Peer) int32 {
	diff := src.finishedNum.Load() - dst.finishedNum.Load()
	if diff > 0 {
		return diff
	}
	return -diff
}

func (peer *Peer) GetParent() *Peer {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.parent
}

func (peer *Peer) GetChildren() *sync.Map {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return &peer.children
}

func (peer *Peer) SetStatus(status PeerStatus) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.status = status
}

func (peer *Peer) BindSendChannel(packetChan chan *scheduler.PeerPacket) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.bindPacketChan = true
	peer.packetChan = packetChan
}

func (peer *Peer) UnBindSendChannel() {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.bindPacketChan {
		if peer.packetChan != nil {
			close(peer.packetChan)
		}
		peer.bindPacketChan = false
	}
}

func (peer *Peer) SendSchedulePacket(packet *scheduler.PeerPacket) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	if peer.bindPacketChan {
		peer.packetChan <- packet
	}
}

func (peer *Peer) IsRunning() bool {
	return peer.status == PeerStatusRunning
}

func (peer *Peer) IsWaiting() bool {
	return peer.status == PeerStatusWaiting
}

func (peer *Peer) IsSuccess() bool {
	return peer.status == PeerStatusSuccess
}

func (peer *Peer) IsDone() bool {
	return peer.status == PeerStatusSuccess || peer.status == PeerStatusFail
}

func (peer *Peer) IsBad() bool {
	return peer.status == PeerStatusFail || peer.status == PeerStatusZombie
}

func (peer *Peer) GetFinishedNum() int32 {
	return peer.finishedNum.Load()
}

func (peer *Peer) GetStatus() PeerStatus {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status
}

func (peer *Peer) MarkLeave() {
	peer.leave.Store(true)
}

func (peer *Peer) IsLeave() bool {
	return peer.leave.Load()
}
