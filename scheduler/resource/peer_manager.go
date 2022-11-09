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
	peer.Task.StorePeer(peer)
	peer.Host.StorePeer(peer)
}

func (p *peerManager) LoadOrStore(peer *Peer) (*Peer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	rawPeer, loaded := p.Map.LoadOrStore(peer.ID, peer)
	if !loaded {
		peer.Host.StorePeer(peer)
		peer.Task.StorePeer(peer)
	}

	return rawPeer.(*Peer), loaded
}

func (p *peerManager) Delete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer, ok := p.Load(key); ok {
		p.Map.Delete(key)
		peer.Task.DeletePeer(key)
		peer.Host.DeletePeer(key)
	}
}

func (p *peerManager) RunGC() error {
	p.Map.Range(func(_, value any) bool {
		peer := value.(*Peer)

		// If the peer state is PeerStateLeave,
		// peer will be reclaimed.
		if peer.FSM.Is(PeerStateLeave) {
			p.Delete(peer.ID)
			peer.Log.Info("peer has been reclaimed")
			return true
		}

		// If the peer's elapsed exceeds the ttl,
		// first set the peer state to PeerStateLeave and then delete peer.
		elapsed := time.Since(peer.UpdateAt.Load())
		if elapsed > p.ttl {
			// If the peer is not leave,
			// first change the state to PeerEventLeave.
			if err := peer.FSM.Event(PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return true
			}

			peer.Log.Info("peer elapsed exceeds the ttl, causing the peer to leave")
			return true
		}

		// If the peer's state is PeerStateFailed,
		// first set the peer state to PeerStateLeave and then delete peer.
		if peer.FSM.Is(PeerStateFailed) {
			if err := peer.FSM.Event(PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return true
			}

			peer.Log.Info("peer state is PeerStateFailed, causing the peer to leave")
		}

		// If no peer exists in the dag of the task,
		// delete the peer.
		degree, err := peer.Task.PeerDegree(peer.ID)
		if err != nil {
			p.Delete(peer.ID)
			peer.Log.Info("peer has been reclaimed")
			return true
		}

		// If the task dag size exceeds the limit,
		// then set the peer state to PeerStateLeave which state is
		// PeerStateSucceeded, and degree is zero.
		if peer.Task.PeerCount() > PeerCountLimitForTask &&
			peer.FSM.Is(PeerStateSucceeded) && degree == 0 {
			if err := peer.FSM.Event(PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return true
			}

			peer.Log.Info("task dag size exceeds the limit, causing the peer to leave")
			return true
		}

		return true
	})

	return nil
}
