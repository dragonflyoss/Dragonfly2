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

	ListPeersByTask(string) []*Peer

	ListPeers() *sync.Map
}

type peerManager struct {
	hostManager              HostManager
	cleanupExpiredPeerTicker *time.Ticker
	peerTTL                  time.Duration
	peerTTI                  time.Duration
	peerMap                  sync.Map
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

func (m *peerManager) ListPeersByTask(taskID string) []*Peer {
	var peers []*Peer
	m.peerMap.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if peer.Task.TaskID == taskID {
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

func (m *peerManager) ListPeers() *sync.Map {
	return &m.peerMap
}

func (m *peerManager) Add(peer *Peer) {
	m.lock.Lock()
	defer m.lock.Unlock()
	peer.Host.AddPeer(peer)
	peer.Task.AddPeer(peer)
	m.peerMap.Store(peer.ID, peer)
}

func (m *peerManager) Get(id string) (*Peer, bool) {
	data, ok := m.peerMap.Load(id)
	if !ok {
		return nil, false
	}
	peer := data.(*Peer)
	return peer, true
}

func (m *peerManager) Delete(id string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	peer, ok := m.Get(id)
	if ok {
		peer.Host.DeletePeer(id)
		peer.Task.DeletePeer(peer)
		peer.ReplaceParent(nil)
		m.peerMap.Delete(id)
	}
	return
}

func (m *peerManager) cleanupPeers() {
	for range m.cleanupExpiredPeerTicker.C {
		m.peerMap.Range(func(key, value interface{}) bool {
			id := key.(string)
			peer := value.(*Peer)
			elapse := time.Since(peer.GetLastAccessTime())
			if elapse > m.peerTTI && !peer.IsDone() && !peer.Host.CDN {
				if !peer.IsConnected() {
					peer.MarkLeave()
				}
				peer.Log().Infof("peer has been more than %s since last access, it's status changes from %s to zombie", m.peerTTI, peer.GetStatus().String())
				peer.SetStatus(PeerStatusZombie)
			}
			if peer.IsLeave() || peer.IsFail() || elapse > m.peerTTL {
				if elapse > m.peerTTL {
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
