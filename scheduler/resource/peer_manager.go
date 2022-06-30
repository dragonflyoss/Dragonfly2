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

//go:generate mockgen -destination peer_manager_mock.go -source peer_manager.go -package resource

package resource

import (
	"sync"
	"time"

	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// GC peer id.
	GCPeerID = "peer"
)

type PeerManager interface {
	// Load returns peer for a key.
	Load(string) (*Peer, bool)

	// Store sets peer.
	Store(*Peer)

	// LoadOrStore returns peer the key if present.
	// Otherwise, it stores and returns the given peer.
	// The loaded result is true if the peer was loaded, false if stored.
	LoadOrStore(*Peer) (*Peer, bool)

	// Delete deletes peer for a key.
	Delete(string)

	// Try to reclaim peer.
	RunGC() error
}

type peerManager struct {
	// Peer sync map.
	*sync.Map

	// Peer time to live.
	ttl time.Duration

	// Peer mutex.
	mu *sync.Mutex
}

// New peer manager interface.
func newPeerManager(cfg *config.GCConfig, gc pkggc.GC) (PeerManager, error) {
	p := &peerManager{
		Map: &sync.Map{},
		ttl: cfg.PeerTTL,
		mu:  &sync.Mutex{},
	}

	if err := gc.Add(pkggc.Task{
		ID:       GCPeerID,
		Interval: cfg.PeerGCInterval,
		Timeout:  cfg.PeerGCInterval,
		Runner:   p,
	}); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *peerManager) Load(key string) (*Peer, bool) {
	rawPeer, ok := p.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawPeer.(*Peer), ok
}

func (p *peerManager) Store(peer *Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Map.Store(peer.ID, peer)
	peer.Host.LoadOrStorePeer(peer)
	peer.Task.LoadOrStorePeer(peer)
}

func (p *peerManager) LoadOrStore(peer *Peer) (*Peer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	rawPeer, loaded := p.Map.LoadOrStore(peer.ID, peer)
	if !loaded {
		peer.Host.LoadOrStorePeer(peer)
		peer.Task.LoadOrStorePeer(peer)
	}

	return rawPeer.(*Peer), loaded
}

func (p *peerManager) Delete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer, ok := p.Load(key); ok {
		p.Map.Delete(key)
		peer.Host.DeletePeer(key)
		peer.Task.DeletePeer(key)
	}
}

func (p *peerManager) RunGC() error {
	p.Map.Range(func(_, value any) bool {
		peer := value.(*Peer)
		elapsed := time.Since(peer.UpdateAt.Load())

		if elapsed > p.ttl && peer.ChildCount.Load() == 0 {
			// If the status is PeerStateLeave,
			// clear peer information.
			if peer.FSM.Is(PeerStateLeave) {
				peer.DeleteParent()
				p.Delete(peer.ID)
				peer.Log.Info("peer has been reclaimed")
				return true
			}

			// If the peer is not leave,
			// first change the state to PeerEventLeave.
			if err := peer.FSM.Event(PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			}

			peer.Log.Info("gc causes the peer to leave")
			return true
		}

		return true
	})

	return nil
}
