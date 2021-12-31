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

package scheduler

import (
	"context"
	"sort"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
)

type Scheduler interface {
	// ScheduleParent schedule a parent and candidates to a peer
	ScheduleParent(context.Context, *entity.Peer, set.SafeSet) ([]*entity.Peer, bool)
}

type scheduler struct {
	evaluator evaluator.Evaluator
}

func New(cfg *config.SchedulerConfig, pluginDir string) Scheduler {
	return &scheduler{
		evaluator: evaluator.New(cfg.Algorithm, pluginDir),
	}
}

func (s *scheduler) ScheduleParent(ctx context.Context, peer *entity.Peer, blocklist set.SafeSet) ([]*entity.Peer, bool) {
	// Only PeerStateRunning peers need to be rescheduled,
	// and other states including the PeerStateBackToSource indicate that
	// they have been scheduled
	if !peer.FSM.Is(entity.PeerStateRunning) {
		peer.Log.Infof("peer state is %s, can not schedule parent", peer.FSM.Current())
		return []*entity.Peer{}, false
	}

	// Find the parent that can be scheduled
	parents := s.findParents(peer, blocklist)
	if len(parents) == 0 {
		peer.Log.Info("can not find parents")
		return []*entity.Peer{}, false
	}

	// Sort parents by evaluation score
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		parents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(peer, parents[i], taskTotalPieceCount) > s.evaluator.Evaluate(peer, parents[j], taskTotalPieceCount)
		},
	)

	// Send scheduling success message
	stream, ok := peer.LoadStream()
	if !ok {
		peer.Log.Error("load peer stream failed")
		return []*entity.Peer{}, false
	}

	if err := stream.Send(constructSuccessPeerPacket(peer, parents[0], parents[1:])); err != nil {
		peer.Log.Error(err)
		return []*entity.Peer{}, false
	}

	peer.ReplaceParent(parents[0])
	peer.Log.Infof("parent schedule succeeded, replace parent to %s", parents[0].ID)
	return parents, true
}

func (s *scheduler) findChildren(peer *entity.Peer, blocklist set.SafeSet) []*entity.Peer {
	var children []*entity.Peer
	peer.Task.Peers.Range(func(_, value interface{}) bool {
		child, ok := value.(*entity.Peer)
		if !ok {
			return true
		}

		if blocklist.Contains(child.ID) {
			peer.Log.Infof("child %s is not selected because it is in blocklist", child.ID)
			return true
		}

		if child == peer {
			peer.Log.Info("child is not selected because it is same")
			return true
		}

		if !child.FSM.Is(entity.PeerStateRunning) {
			peer.Log.Infof("child %s is not selected because its state is %s", child.ID, child.FSM.Current())
			return true
		}

		if child.IsAncestor(peer) {
			peer.Log.Infof("child %s is not selected because it is ancestor", child.ID)
			return true
		}

		if child.Host.IsCDN {
			peer.Log.Infof("child %s is not selected because it is cdn", child.ID)
			return true
		}

		if child.Pieces.Count() >= peer.Pieces.Count() {
			peer.Log.Infof("child %s is not selected because its pieces count is greater than peer pieces count", child.ID)
			return true
		}

		if parent, ok := child.LoadParent(); ok && !s.evaluator.IsBadNode(parent) {
			peer.Log.Infof("child %s is not selected because its parent is not bad node", child.ID)
			return true
		}

		children = append(children, child)
		peer.Log.Infof("child %s is selected", child.ID)
		return true
	})

	return children
}

func (s *scheduler) findParents(peer *entity.Peer, blocklist set.SafeSet) []*entity.Peer {
	var parents []*entity.Peer
	peer.Task.Peers.Range(func(_, value interface{}) bool {
		parent, ok := value.(*entity.Peer)
		if !ok {
			return true
		}

		if blocklist.Contains(parent.ID) {
			peer.Log.Infof("parent %s is not selected because it is in blocklist", parent.ID)
			return true
		}

		if parent == peer {
			peer.Log.Info("child is not selected because it is same")
			return true
		}

		if s.evaluator.IsBadNode(parent) {
			peer.Log.Infof("parent %s is not selected because it is bad node", parent.ID)
			return true
		}

		if parent.IsDescendant(peer) {
			peer.Log.Infof("parent %s is not selected because it is descendant", parent.ID)
			return true
		}

		if parent.Host.FreeUploadLoad() <= 0 {
			peer.Log.Infof("parent %s is not selected because its free upload is empty", parent.ID)
			return true
		}

		if parent.Pieces.Count() <= peer.Pieces.Count() {
			peer.Log.Infof("parent %s is not selected because its pieces count is less than peer pieces count", parent.ID)
			return true
		}

		parents = append(parents, parent)
		peer.Log.Infof("parent %s is selected", parent.ID)
		return true
	})

	return parents
}

func constructSuccessPeerPacket(peer *entity.Peer, parent *entity.Peer, candidateParents []*entity.Peer) *rpcscheduler.PeerPacket {
	var stealPeers []*rpcscheduler.PeerPacket_DestPeer
	for _, candidateParent := range candidateParents {
		stealPeers = append(stealPeers, &rpcscheduler.PeerPacket_DestPeer{
			Ip:      candidateParent.Host.IP,
			RpcPort: candidateParent.Host.Port,
			PeerId:  candidateParent.ID,
		})
	}

	return &rpcscheduler.PeerPacket{
		TaskId: peer.Task.ID,
		SrcPid: peer.ID,
		// TODO(gaius-qi) Configure ParallelCount parameter in manager service
		ParallelCount: 1,
		MainPeer: &rpcscheduler.PeerPacket_DestPeer{
			Ip:      parent.Host.IP,
			RpcPort: parent.Host.Port,
			PeerId:  parent.ID,
		},
		StealPeers: stealPeers,
		Code:       base.Code_Success,
	}
}
