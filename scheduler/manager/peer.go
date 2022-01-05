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

package manager

import (
	"sync"
	"time"

	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
)

const (
	// GC peer id
	GCPeerID = "peer"
)

type Peer interface {
	// Load return peer entity for a key
	Load(string) (*entity.Peer, bool)

	// Store set peer entity
	Store(*entity.Peer)

	// LoadOrStore returns peer entity the key if present.
	// Otherwise, it stores and returns the given peer entity.
	// The loaded result is true if the peer entity was loaded, false if stored.
	LoadOrStore(*entity.Peer) (*entity.Peer, bool)

	// Delete deletes peer entity for a key
	Delete(string)
}

type peer struct {
	// Peer sync map
	*sync.Map

	// Peer time to live
	ttl time.Duration

	// Peer mutex
	mu *sync.Mutex
}

// New peer interface
func newPeer(cfg *config.GCConfig, gc pkggc.GC) (Peer, error) {
	p := &peer{
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

func (p *peer) Load(key string) (*entity.Peer, bool) {
	rawPeer, ok := p.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawPeer.(*entity.Peer), ok
}

func (p *peer) Store(peer *entity.Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Map.Store(peer.ID, peer)
	peer.Host.StorePeer(peer)
	peer.Task.StorePeer(peer)
}

func (p *peer) LoadOrStore(peer *entity.Peer) (*entity.Peer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	rawPeer, loaded := p.Map.LoadOrStore(peer.ID, peer)
	if !loaded {
		peer.Host.StorePeer(peer)
		peer.Task.StorePeer(peer)
	}

	return rawPeer.(*entity.Peer), loaded
}

func (p *peer) Delete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer, ok := p.Load(key); ok {
		p.Map.Delete(key)
		peer.Host.DeletePeer(key)
		peer.Task.DeletePeer(key)
	}
}

func (p *peer) RunGC() error {
	p.Map.Range(func(_, value interface{}) bool {
		peer := value.(*entity.Peer)
		elapsed := time.Since(peer.UpdateAt.Load())

		if elapsed > p.ttl && peer.LenChildren() == 0 {
			// If the status is PeerStateLeave,
			// clear peer information
			if peer.FSM.Is(entity.PeerStateLeave) {
				peer.DeleteParent()
				p.Delete(peer.ID)
				peer.Log.Info("peer has been reclaimed")
				return true
			}

			// If the peer is not leave,
			// first change the state to PeerEventLeave
			if err := peer.FSM.Event(entity.PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %v", err)
			}
			peer.Log.Info("gc causes the peer to leave")

			return true
		}

		return true
	})

	return nil
}
