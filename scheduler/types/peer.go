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
	PeerStatusWaiting PeerStatus = iota + 1
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
	// FinishedNum specifies downloaded finished piece number
	FinishedNum    int32
	LastAccessTime time.Time
	Parent         *PeerNode
	Children       map[string]*PeerNode
	Status         PeerStatus
	CostHistory    []int
	PacketChan     chan *scheduler.PeerPacket
}

func IsSuccessPeer(peer *PeerNode) bool {
	return peer.Status == PeerStatusSuccess
}

func IsDonePeer(peer *PeerNode) bool {
	return peer.Status == PeerStatusSuccess || peer.Status == PeerStatusBadNode
}

func (peer *PeerNode) GetWholeTreeNode() int {
	peer.lock.RLock()
	defer peer.lock.RUnlock()
	count := len(peer.Children)
	for _, peerNode := range peer.Children {
		count += peerNode.GetWholeTreeNode()
	}
	return count
}

func (peer *PeerNode) AddChild(child *PeerNode) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.Children[child.PeerID] = child
	peer.Host.CurrentUploadLoad--
}

func (peer *PeerNode) DeleteChild(child *PeerNode) {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	peer.deleteChild(child)
}

func (peer *PeerNode) deleteChild(child *PeerNode) {
	delete(peer.Children, child.PeerID)
	peer.Host.CurrentUploadLoad++
}

// ReplaceParent replace parent
// delete peer from parent
func (peer *PeerNode) ReplaceParent(parent *PeerNode) error {
	peer.lock.Lock()
	defer peer.lock.Unlock()
	oldParent := peer.Parent
	if oldParent != nil {
		oldParent.deleteChild(peer)
	}
	peer.Parent = parent
	if parent != nil {
		parent.AddChild(peer)
	}
	return nil
}

func (peer *PeerNode) GetCost() int {
	if len(peer.CostHistory) < 1 {
		return int(time.Second / time.Millisecond)
	}
	totalCost := 0
	for _, cost := range peer.CostHistory {
		totalCost += cost
	}
	return totalCost / len(peer.CostHistory)
}

func (peer *PeerNode) AddPieceStatus(ps *scheduler.PieceResult) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if !ps.Success {
		return
	}

	peer.FinishedNum = ps.FinishedCount

	peer.addCost(int(ps.EndTime - ps.BeginTime))
	peer.Task.PeerNodes.Update(peer)
}

func (peer *PeerNode) addCost(cost int) {
	peer.CostHistory = append(peer.CostHistory, cost)
	if len(peer.CostHistory) > 20 {
		peer.CostHistory = peer.CostHistory[1:]
	}
}

func (peer *PeerNode) GetDepth() int {
	var deep int
	node := peer
	for node != nil {
		deep++
		if node.Parent == nil || IsCDNHost(node.Host) {
			break
		}
		node = node.Parent
	}
	return deep
}

func (peer *PeerNode) GetTreeRoot() *PeerNode {
	node := peer
	for node != nil {
		if node.Parent == nil || IsCDNHost(node.Host) {
			break
		}
		node = node.Parent
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
		if node.Parent == nil || IsCDNHost(node.Host) {
			return false
		} else if node.PeerID == ancestor.PeerID {
			return true
		}
		node = node.Parent
	}
	return false
}

func (peer *PeerNode) IsWaiting() bool {
	if peer.Parent == nil {
		return false
	}

	return peer.FinishedNum >= peer.Parent.FinishedNum
}

func (peer *PeerNode) GetSortKeys() (key1, key2 int) {
	key1 = int(peer.FinishedNum)
	key2 = int(peer.getFreeLoad())
	return
}

func (peer *PeerNode) getFreeLoad() int {
	if peer.Host == nil {
		return 0
	}
	return peer.Host.GetFreeUploadLoad()
}

func GetDiffPieceNum(src *PeerNode, dst *PeerNode) int32 {
	diff := src.FinishedNum - dst.FinishedNum
	if diff > 0 {
		return diff
	}
	return -diff
}

func IsRunning(peer *PeerNode) bool {
	return peer.Status != PeerStatusBadNode
}

func (peer *PeerNode) GetParent() *PeerNode {
	return peer.Parent
}

func (peer *PeerNode) GetChildren() map[string]*PeerNode {
	return peer.Children
}

func (peer *PeerNode) SetStatus(status PeerStatus) {
	peer.Status = status
}

func (peer *PeerNode) SetSendChannel(packetChan chan *scheduler.PeerPacket) {
	peer.PacketChan = packetChan
}

func (peer *PeerNode) GetSendChannel() chan *scheduler.PeerPacket {
	return peer.PacketChan
}
