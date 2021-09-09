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

//go:generate mockgen -destination ./mock/mock_peer_mgr.go -package mock d7y.io/dragonfly/v2/scheduler/supervisor PeerMgr

package supervisor

import (
	"sync"
)

type PeerMgr interface {
	Add(peer *Peer)

	Get(peerID string) (*Peer, bool)

	Delete(peerID string)

	ListPeersByTask(taskID string) []*Peer

	ListPeers() *sync.Map
}
