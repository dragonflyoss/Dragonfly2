/*
 *     Copyright 2023 The Dragonfly Authors
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

package pex

import (
	"sync"

	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
)

type peerPool struct {
	lock *sync.RWMutex
	data map[string]map[string]*schedulerv1.PeerPacket_DestPeer
}

func newPeerPool() *peerPool {
	return &peerPool{
		lock: &sync.RWMutex{},
		data: map[string]map[string]*schedulerv1.PeerPacket_DestPeer{},
	}
}

func (p *peerPool) Sync(nodeMeta *MemberMeta, peer *dfdaemonv1.PeerMetadata) {
	p.lock.Lock()
	defer p.lock.Unlock()

	peers, ok := p.data[peer.TaskId]
	if !ok {
		p.data[peer.TaskId] = map[string]*schedulerv1.PeerPacket_DestPeer{}
	}

	switch peer.State {
	case dfdaemonv1.PeerState_Unknown:
		return
	case dfdaemonv1.PeerState_Running, dfdaemonv1.PeerState_Success:
		peers[peer.PeerId] = &schedulerv1.PeerPacket_DestPeer{
			Ip:      nodeMeta.IP,
			RpcPort: nodeMeta.RpcPort,
			PeerId:  peer.PeerId,
		}
	case dfdaemonv1.PeerState_Failed, dfdaemonv1.PeerState_Deleted:
		delete(peers, peer.PeerId)
		// clean task map
		if len(peers) == 0 {
			delete(p.data, peer.TaskId)
		}
	default:
		return
	}
}

func (p *peerPool) Find(task string) ([]*schedulerv1.PeerPacket_DestPeer, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	peers, ok := p.data[task]
	if !ok {
		return nil, false
	}
	var dp []*schedulerv1.PeerPacket_DestPeer
	for _, peer := range peers {
		dp = append(dp, peer)
	}
	return dp, ok
}
