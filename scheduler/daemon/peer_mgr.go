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

package daemon

import (
	"sync"

	"d7y.io/dragonfly/v2/scheduler/types/peer"
	"d7y.io/dragonfly/v2/scheduler/types/task"
)

type PeerMgr interface {
	Add(peer *peer.PeerNode)

	Get(peerID string) (*peer.PeerNode, bool)

	Delete(peerID string)

	ListPeers() *sync.Map

	ListPeerNodesByTask(taskID string) []*peer.PeerNode

	Pick(task *task.Task, limit int, pickFn func(pt *peer.PeerNode) bool) []*peer.PeerNode

	PickReverse(task *task.Task, limit int, pickFn func(peerNode *peer.PeerNode) bool) []*peer.PeerNode
}
