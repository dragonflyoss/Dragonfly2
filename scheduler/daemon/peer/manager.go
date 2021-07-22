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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type manager struct {
	hostManager              daemon.HostMgr
	cleanupExpiredPeerTicker *time.Ticker
	peerTTL                  time.Duration
	peerMap                  sync.Map
}

func (m *manager) ListPeers() *sync.Map {
	return &m.peerMap
}

func NewManager(cfg *config.GCConfig, hostManager daemon.HostMgr) daemon.PeerMgr {
	m := &manager{
		hostManager:              hostManager,
		cleanupExpiredPeerTicker: time.NewTicker(cfg.PeerGCInterval),
		peerTTL:                  cfg.PeerTTL,
	}
	go m.cleanupPeers()
	return m
}

var _ daemon.PeerMgr = (*manager)(nil)

func (m *manager) Add(peer *types.Peer) {
	peer.Host.AddPeer(peer)
	peer.Task.AddPeer(peer)
	m.peerMap.Store(peer.PeerID, peer)
}

func (m *manager) Get(peerID string) (*types.Peer, bool) {
	data, ok := m.peerMap.Load(peerID)
	if !ok {
		return nil, false
	}
	peer := data.(*types.Peer)
	return peer, true
}

func (m *manager) Delete(peerID string) {
	peer, ok := m.Get(peerID)
	if ok {
		peer.Host.DeletePeer(peerID)
		peer.Task.DeletePeer(peer)
		if peer.PacketChan != nil {
			close(peer.PacketChan)
		}
		m.peerMap.Delete(peerID)
	}
	return
}

func (m *manager) ListPeersByTask(taskID string) (peers []*types.Peer) {
	m.peerMap.Range(func(key, value interface{}) bool {
		peer := value.(*types.Peer)
		if peer.Task.TaskID == taskID {
			peers = append(peers, peer)
		}
		return true
	})
	return
}

func (m *manager) Pick(task *types.Task, limit int, pickFn func(peer *types.Peer) bool) (pickedPeers []*types.Peer) {
	return m.pick(task, limit, false, pickFn)
}

func (m *manager) PickReverse(task *types.Task, limit int, pickFn func(peer *types.Peer) bool) (pickedPeers []*types.Peer) {
	return m.pick(task, limit, true, pickFn)
}

func (m *manager) pick(task *types.Task, limit int, reverse bool, pickFn func(peer *types.Peer) bool) (pickedPeers []*types.Peer) {
	if pickFn == nil {
		return
	}
	if !reverse {
		task.ListPeers().Range(func(data sortedlist.Item) bool {
			if len(pickedPeers) >= limit {
				return false
			}
			peer := data.(*types.Peer)
			if pickFn(peer) {
				pickedPeers = append(pickedPeers, peer)
			}
			return true
		})
		return
	}
	task.ListPeers().RangeReverse(func(data sortedlist.Item) bool {
		if len(pickedPeers) >= limit {
			return false
		}
		peer := data.(*types.Peer)
		if pickFn(peer) {
			pickedPeers = append(pickedPeers, peer)
		}
		return true
	})
	return
}

func (m *manager) cleanupPeers() {
	for range m.cleanupExpiredPeerTicker.C {
		m.peerMap.Range(func(key, value interface{}) bool {
			peer := value.(*types.Peer)
			if time.Now().Sub(peer.GetLastAccessTime()) > m.peerTTL {
				logger.Debugf("peer %s has been more than %s since last access, set status to zombie", peer.PeerID, m.peerTTL)
				peer.SetStatus(types.PeerStatusZombie)
			}
			if peer.IsLeave() {
				logger.Debugf("delete peer %s", peer.PeerID)
				m.Delete(key.(string))
				if !peer.Host.CDN && peer.Host.GetPeerTaskNum() == 0 {
					m.hostManager.Delete(peer.Host.UUID)
				}
			}
			return true
		})
	}
}
