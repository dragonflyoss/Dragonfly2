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

package peer

import (
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types/peer"
	"d7y.io/dragonfly/v2/scheduler/types/task"
)

const (
//PeerGoneTimeout      = 10 * time.Second
//PeerForceGoneTimeout = 2 * time.Minute
)

type manager struct {
	peerMap sync.Map
	//downloadMonitorQueue workqueue.DelayingInterface
	taskManager daemon.TaskMgr
	hostManager daemon.HostMgr
	verbose     bool
}

func (m *manager) ListPeers() *sync.Map {
	return &m.peerMap
}

func NewManager() daemon.PeerMgr {

	return &manager{
		//downloadMonitorQueue: workqueue.NewDelayingQueue(),
		//taskManager: taskManager,
		//hostManager: hostManager,
	}

	//go peerManager.downloadMonitorWorkingLoop()
	//
	//go peerManager.gcWorkingLoop()
	//
	//go peerManager.printDebugInfoLoop()
}

var _ daemon.PeerMgr = (*manager)(nil)

func (m *manager) Add(peerNode *peer.PeerNode) {
	peerNode.LastAccessTime = time.Now()
	peerNode.Host.AddPeerNode(peerNode)
	peerNode.Task.PeerNodes.Add(peerNode)
	m.peerMap.Store(peerNode.PeerID, peerNode)
	peerNode.Task.AddPeerNode(peerNode)
}

func (m *manager) Get(peerID string) (*peer.PeerNode, bool) {
	data, ok := m.peerMap.Load(peerID)
	if !ok {
		return nil, false
	}
	peerNode := data.(*peer.PeerNode)
	peerNode.LastAccessTime = time.Now()
	return peerNode, true
}

func (m *manager) Delete(peerID string) {
	peer, ok := m.Get(peerID)
	if ok {
		peer.Host.DeletePeerNode(peerID)
		peer.Task.PeerNodes.Delete(peer)
		m.peerMap.Delete(peerID)
	}
	return
}

func (m *manager) ListPeerNodesByTask(taskID string) (peers []*peer.PeerNode) {
	m.peerMap.Range(func(key, value interface{}) bool {
		peer := value.(*peer.PeerNode)
		if peer.Task.TaskID == taskID {
			peers = append(peers, peer)
		}
		return true
	})
	return
}

func (m *manager) Pick(task *task.Task, limit int, pickFn func(peer *peer.PeerNode) bool) (pickedPeers []*peer.PeerNode) {
	return m.pick(task, limit, false, pickFn)
}

func (m *manager) PickReverse(task *task.Task, limit int, pickFn func(peer *peer.PeerNode) bool) (pickedPeers []*peer.PeerNode) {
	return m.pick(task, limit, true, pickFn)
}

func (m *manager) pick(task *task.Task, limit int, reverse bool, pickFn func(peer *peer.PeerNode) bool) (pickedPeers []*peer.PeerNode) {
	if pickFn == nil {
		return
	}
	if !reverse {
		task.PeerNodes.Range(func(data sortedlist.Item) bool {
			if len(pickedPeers) >= limit {
				return false
			}
			peer := data.(*peer.PeerNode)
			if pickFn(peer) {
				pickedPeers = append(pickedPeers, peer)
			}
			return true
		})
		return
	}
	task.PeerNodes.RangeReverse(func(data sortedlist.Item) bool {
		if len(pickedPeers) >= limit {
			return false
		}
		peer := data.(*peer.PeerNode)
		if pickFn(peer) {
			pickedPeers = append(pickedPeers, peer)
		}
		return true
	})
	return
}
