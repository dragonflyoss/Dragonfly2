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

	"d7y.io/dragonfly/v2/scheduler/config"
)

type PeerManager interface {
	Add(*Peer)

	Get(string) (*Peer, bool)

	Delete(string)

	GetPeersByTask(string) []*Peer

	GetPeers() *sync.Map
}

type peerManager struct {
	hostManager              HostManager
	cleanupExpiredPeerTicker *time.Ticker
	peerTTL                  time.Duration
	peerTTI                  time.Duration
	peers                    *sync.Map
	lock                     sync.RWMutex
}

func NewPeerManager(cfg *config.GCConfig, hostManager HostManager) PeerManager {
	m := &peerManager{
		hostManager:              hostManager,
		cleanupExpiredPeerTicker: time.NewTicker(cfg.PeerGCInterval),
		peerTTL:                  cfg.PeerTTL,
		peerTTI:                  cfg.PeerTTI,
	}
	go m.cleanupPeers()
	return m
}

func (m *peerManager) Add(peer *Peer) {
	m.lock.Lock()
	defer m.lock.Unlock()
	peer.Host.AddPeer(peer)
	peer.Task.AddPeer(peer)
	m.peers.Store(peer.ID, peer)
}

func (m *peerManager) Get(id string) (*Peer, bool) {
	peer, ok := m.peers.Load(id)
	if !ok {
		return nil, false
	}

	return peer.(*Peer), true
}

func (m *peerManager) Delete(id string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if peer, ok := m.Get(id); ok {
		peer.Host.DeletePeer(id)
		peer.Task.DeletePeer(peer)
		peer.ReplaceParent(nil)
		m.peers.Delete(id)
	}
}

func (m *peerManager) GetPeersByTask(taskID string) []*Peer {
	var peers []*Peer
	m.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if peer.Task.TaskID == taskID {
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

func (m *peerManager) GetPeers() *sync.Map {
	return m.peers
}

func (m *peerManager) cleanupPeers() {
	for range m.cleanupExpiredPeerTicker.C {
		m.peers.Range(func(key, value interface{}) bool {
			id := key.(string)
			peer := value.(*Peer)
			elapsed := time.Since(peer.GetLastAccessTime())

			if elapsed > m.peerTTI && !peer.IsDone() && !peer.Host.IsCDN {
				if !peer.IsConnected() {
					peer.Leave()
				}
				peer.Log().Infof("peer has been more than %s since last access, it's status changes from %s to zombie", m.peerTTI, peer.GetStatus().String())
				peer.SetStatus(PeerStatusZombie)
			}

			if peer.IsLeave() || peer.IsFail() || elapsed > m.peerTTL {
				if elapsed > m.peerTTL {
					peer.Log().Infof("delete peer because %s have passed since last access", m.peerTTL)
				}
				m.Delete(id)
				if peer.Host.GetPeersLen() == 0 {
					m.hostManager.Delete(peer.Host.UUID)
				}
				if peer.Task.ListPeers().Size() == 0 {
					peer.Task.Log().Info("peers is empty, task status become waiting")
					peer.Task.SetStatus(TaskStatusWaiting)
				}
			}

			return true
		})
	}
}
