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

	"d7y.io/dragonfly/v2/scheduler/entity"
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

	// Peer mutex
	mu *sync.Mutex
}

// New peer interface
func newPeer() Peer {
	return &peer{
		Map: &sync.Map{},
		mu:  &sync.Mutex{},
	}
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
