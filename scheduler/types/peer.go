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
)

type PeerStatus uint8

const (
	PeerStatusWaiting PeerStatus = iota
	PeerStatusRunning
	PeerStatusNeedParent
	PeerStatusNeedChildren
	PeerStatusBadNode
	PeerStatusNeedAdjustNode
	PeerStatusNeedCheckNode
	PeerStatusSuccess
	PeerStatusLeaveNode
	PeerStatusAddParent
	PeerStatusNodeGone
	PeerStatusZombie
)

type PeerNode struct {
	lock sync.RWMutex
	// PeerID specifies ID of peer
	PeerID string
	// Task specifies
	Task *Task
	// Host specifies
	Host *NodeHost
	// finishedNum specifies downloaded finished piece number
	finishedNum    int32
	lastAccessTime time.Time
	parent         *PeerNode
	children       map[string]*PeerNode
	status         PeerStatus
	costHistory    []int
	PacketChan     chan *scheduler.PeerPacket
}

func NewPeerNode(peerID string, task *Task, host *NodeHost) *PeerNode {
	return &PeerNode{
		PeerID:         peerID,
		Task:           task,
		Host:           host,
		lastAccessTime: time.Now(),
		status:         PeerStatusWaiting,
	}
}

func (peer *PeerNode) GetWholeTreeNode() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	count := len(peer.children)
	for _, peerNode := range peer.children {
		count += peerNode.GetWholeTreeNode()
	}
	return count
}

func (peer *PeerNode) GetLastAccessTime() time.Time {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.lastAccessTime
}

func (peer *PeerNode) AddChild(child *PeerNode) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.addChild(child)
}

func (peer *PeerNode) DeleteChild(child *PeerNode) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.deleteChild(child)
}

func (peer *PeerNode) deleteChild(child *PeerNode) {
	delete(peer.children, child.PeerID)
	peer.Host.IncUploadLoad()
}

func (peer *PeerNode) addChild(child *PeerNode) {
	peer.children[child.PeerID] = child
	peer.Host.DesUploadLoad()
}

func (peer *PeerNode) ReplaceParent(parent *PeerNode) error {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	oldParent := peer.parent
	if oldParent != nil {
		oldParent.DeleteChild(peer)
	}
	peer.parent = parent
	if parent != nil {
		parent.AddChild(peer)
	}
	return nil
}

func (peer *PeerNode) GetCostHistory() []int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.costHistory
}

func (peer *PeerNode) GetCost() int {
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

func (peer *PeerNode) AddPieceStatus(ps *scheduler.PieceResult) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if !ps.Success {
		return
	}

	peer.finishedNum = ps.FinishedCount

	peer.addCost(int(ps.EndTime - ps.BeginTime))
	peer.Task.peerNodes.Update(peer)
}

func (peer *PeerNode) addCost(cost int) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.costHistory = append(peer.costHistory, cost)
	if len(peer.costHistory) > 20 {
		peer.costHistory = peer.costHistory[len(peer.costHistory)-20:]
	}
}

func (peer *PeerNode) GetDepth() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	var deep int
	node := peer
	for node != nil {
		deep++
		if node.parent == nil || node.Host.IsCDNHost() {
			break
		}
		node = node.parent
	}
	return deep
}

func (peer *PeerNode) GetTreeRoot() *PeerNode {
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.IsCDNHost() {
			break
		}
		node = node.parent
	}
	return node
}

// if ancestor is ancestor of peer
func (peer *PeerNode) IsAncestor(ancestor *PeerNode) bool {
	if ancestor == nil {
		return false
	}
	node := peer
	for node != nil {
		if node.parent == nil || node.Host.IsCDNHost() {
			return false
		} else if node.PeerID == ancestor.PeerID {
			return true
		}
		node = node.parent
	}
	return false
}

func (peer *PeerNode) IsWaiting() bool {
	peer.lock.RLock()
	defer peer.lock.RLock()
	if peer.parent == nil {
		return false
	}
	return peer.finishedNum >= peer.parent.finishedNum
}

func (peer *PeerNode) GetSortKeys() (key1, key2 int) {
	peer.lock.RLock()
	defer peer.lock.RLock()
	key1 = int(peer.finishedNum)
	key2 = int(peer.getFreeLoad())
	return
}

func (peer *PeerNode) getFreeLoad() int {
	peer.lock.RLock()
	defer peer.lock.RLock()
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
}

func (peer *PeerNode) getFinishNum() int32 {
	peer.lock.RLock()
	defer peer.lock.RLock()
	return peer.finishedNum
}

func GetDiffPieceNum(src *PeerNode, dst *PeerNode) int32 {
	diff := src.finishedNum - dst.finishedNum
	if diff > 0 {
		return diff
	}
	return -diff
}

func (peer *PeerNode) GetParent() *PeerNode {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.parent
}

func (peer *PeerNode) GetChildren() map[string]*PeerNode {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.children
}

func (peer *PeerNode) SetStatus(status PeerStatus) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.status = status
}

func (peer *PeerNode) SetSendChannel(packetChan chan *scheduler.PeerPacket) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.PacketChan = packetChan
}

func (peer *PeerNode) GetSendChannel() chan *scheduler.PeerPacket {
	return peer.PacketChan
}

func (peer *PeerNode) IsRunning() bool {
	return peer.status != PeerStatusBadNode
}

func (peer *PeerNode) IsSuccess() bool {
	return peer.status == PeerStatusSuccess
}

func (peer *PeerNode) IncFinishNum() {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.finishedNum++
}

func (peer *PeerNode) IsDone() bool {
	return peer.status == PeerStatusSuccess || peer.status == PeerStatusBadNode
}

func (peer *PeerNode) Touch() {
	peer.lastAccessTime = time.Now()
	peer.Task.lastAccessTime = time.Now()
}

func (peer *PeerNode) GetFinishedNum() int32 {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.finishedNum
}

func (peer *PeerNode) GetStatus() interface{} {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	return peer.status
}
