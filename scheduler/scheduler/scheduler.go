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

//go:generate mockgen -destination mocks/scheduler_mock.go -source scheduler.go -package mocks

package scheduler

import (
	"context"
	"sort"
	"time"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
)

const (
	// Default tree available depth
	defaultAvailableDepth = 2

	// Default tree depth limit
	defaultDepthLimit = 4
)

type Scheduler interface {
	// ScheduleParent schedule a parent and candidates to a peer
	ScheduleParent(context.Context, *resource.Peer, set.SafeSet)

	// Find the parent that best matches the evaluation and notify peer
	NotifyAndFindParent(context.Context, *resource.Peer, set.SafeSet) ([]*resource.Peer, bool)

	// Find the parent that best matches the evaluation
	FindParent(context.Context, *resource.Peer, set.SafeSet) (*resource.Peer, bool)
}

type scheduler struct {
	// Evaluator interface
	evaluator evaluator.Evaluator

	// Scheduler configuration
	config *config.SchedulerConfig

	// Scheduler dynamic configuration
	dynconfig config.DynconfigInterface
}

func New(cfg *config.SchedulerConfig, dynconfig config.DynconfigInterface, pluginDir string) Scheduler {
	return &scheduler{
		evaluator: evaluator.New(cfg.Algorithm, pluginDir),
		config:    cfg,
		dynconfig: dynconfig,
	}
}

// ScheduleParent schedule a parent and candidates to a peer
func (s *scheduler) ScheduleParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) {
	var n int
	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return
		default:
		}

		// If the scheduling exceeds the RetryBackSourceLimit or the latest cdn peer state is PeerStateFailed,
		// peer will download the task back-to-source
		if (n >= s.config.RetryBackSourceLimit || peer.Task.IsCDNFailed()) &&
			peer.Task.CanBackToSource() {
			stream, ok := peer.LoadStream()
			if !ok {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer back-to-source
			if err := stream.Send(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource}); err != nil {
				peer.Log.Errorf("send packet failed: %v", err)
				return
			}
			peer.Log.Infof("peer scheduling %d times, peer downloads back-to-source %d",
				n, base.Code_SchedNeedBackSource)

			if err := peer.FSM.Event(resource.PeerEventDownloadFromBackToSource); err != nil {
				peer.Log.Errorf("peer fsm event failed: %v", err)
				return
			}

			// If the task state is TaskStateFailed,
			// peer back-to-source and reset task state to TaskStateRunning
			if peer.Task.FSM.Is(resource.TaskStateFailed) {
				if err := peer.Task.FSM.Event(resource.TaskEventDownload); err != nil {
					peer.Task.Log.Errorf("task fsm event failed: %v", err)
					return
				}
			}

			return
		}

		// Handle peer schedule failed
		if n >= s.config.RetryLimit {
			stream, ok := peer.LoadStream()
			if !ok {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer schedule failed
			if err := stream.Send(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}); err != nil {
				peer.Log.Errorf("send packet failed: %v", err)
				return
			}
			peer.Log.Errorf("peer scheduling exceeds the limit %d times and return code %d", s.config.RetryLimit, base.Code_SchedTaskStatusError)
			return
		}

		if _, ok := s.NotifyAndFindParent(ctx, peer, blocklist); !ok {
			n++
			peer.Log.Infof("schedule parent %d times failed", n)

			// Sleep to avoid hot looping
			time.Sleep(s.config.RetryInterval)
			continue
		}

		peer.Log.Infof("schedule parent %d times successfully", n+1)
		return
	}
}

// NotifyAndFindParent finds parent that best matches the evaluation and notify peer
func (s *scheduler) NotifyAndFindParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) ([]*resource.Peer, bool) {
	// Only PeerStateRunning peers need to be rescheduled,
	// and other states including the PeerStateBackToSource indicate that
	// they have been scheduled
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer state is %s, can not schedule parent", peer.FSM.Current())
		return []*resource.Peer{}, false
	}

	// Find the parent that can be scheduled
	parents := s.filterParents(peer, blocklist)
	if len(parents) == 0 {
		peer.Log.Info("can not find parents")
		return []*resource.Peer{}, false
	}

	// Sort parents by evaluation score
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		parents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(parents[i], peer, taskTotalPieceCount) > s.evaluator.Evaluate(parents[j], peer, taskTotalPieceCount)
		},
	)

	// Send scheduling success message
	stream, ok := peer.LoadStream()
	if !ok {
		peer.Log.Error("load peer stream failed")
		return []*resource.Peer{}, false
	}

	if err := stream.Send(constructSuccessPeerPacket(s.dynconfig, peer, parents[0], parents[1:])); err != nil {
		peer.Log.Error(err)
		return []*resource.Peer{}, false
	}

	// Add steal peers to current peer
	peer.StealPeers.Clear()
	for _, parent := range parents[1:] {
		peer.StealPeers.Add(parent.ID)
	}

	// Replace peer's parent with scheduled parent
	peer.ReplaceParent(parents[0])
	peer.Log.Infof("schedule parent successful, replace parent to %s and steal peers is %v",
		parents[0].ID, peer.StealPeers.Values())
	return parents, true
}

// FindParent finds parent that best matches the evaluation
func (s *scheduler) FindParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) (*resource.Peer, bool) {
	// Filter the parent that can be scheduled
	parents := s.filterParents(peer, blocklist)
	if len(parents) == 0 {
		peer.Log.Info("can not find parents")
		return nil, false
	}

	// Sort parents by evaluation score
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		parents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(parents[i], peer, taskTotalPieceCount) > s.evaluator.Evaluate(parents[j], peer, taskTotalPieceCount)
		},
	)

	peer.Log.Infof("find parent %s successful", parents[0].ID)
	return parents[0], true
}

// Filter the parent that can be scheduled
func (s *scheduler) filterParents(peer *resource.Peer, blocklist set.SafeSet) []*resource.Peer {
	filterParentLimit := config.DefaultSchedulerFilterParentLimit
	if config, ok := s.dynconfig.GetSchedulerClusterConfig(); ok && filterParentLimit > 0 {
		filterParentLimit = int(config.FilterParentLimit)
	}

	var parents []*resource.Peer
	var parentIDs []string
	var n int
	peer.Task.Peers.Range(func(_, value interface{}) bool {
		if n > filterParentLimit {
			return false
		}
		n++

		parent, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		if blocklist.Contains(parent.ID) {
			peer.Log.Debugf("parent %s is not selected because it is in blocklist", parent.ID)
			return true
		}

		if parent.StealPeers.Contains(peer.ID) {
			peer.Log.Debugf("parent %s is not selected because it is in steal peers", parent.ID)
			return true
		}

		if parent.ID == peer.ID {
			peer.Log.Debug("parent is not selected because it is same")
			return true
		}

		if s.evaluator.IsBadNode(parent) {
			peer.Log.Debugf("parent %s is not selected because it is bad node", parent.ID)
			return true
		}

		peerChildCount := peer.ChildCount.Load()
		parentDepth := parent.Depth()
		if peerChildCount > 0 && parentDepth > defaultAvailableDepth {
			peer.Log.Debugf("peer has %d children and parent %s depth is %d", peerChildCount, parent.ID, parentDepth)
			return true
		}

		peerDepth := peer.Depth()
		if parentDepth+peerDepth > defaultDepthLimit {
			peer.Log.Debugf("exceeds the %d depth limit of the tree, peer depth is %d, parent %s is %d", defaultDepthLimit, peerDepth, parent.ID, parentDepth)
			return true
		}

		if parent.IsDescendant(peer) {
			peer.Log.Debugf("parent %s is not selected because it is descendant", parent.ID)
			return true
		}

		if parent.IsAncestor(peer) {
			peer.Log.Debugf("parent %s is not selected because it is ancestor", parent.ID)
			return true
		}

		if parent.Host.FreeUploadLoad() <= 0 {
			peer.Log.Debugf("parent %s is not selected because its free upload is empty, upload limit is %d, upload peer count is %d",
				parent.ID, parent.Host.UploadLoadLimit.Load(), parent.Host.UploadPeerCount.Load())
			return true
		}

		parents = append(parents, parent)
		parentIDs = append(parentIDs, parent.ID)
		return true
	})

	peer.Log.Infof("candidate parents include %#v", parentIDs)
	return parents
}

// Construct peer successful packet
func constructSuccessPeerPacket(dynconfig config.DynconfigInterface, peer *resource.Peer, parent *resource.Peer, candidateParents []*resource.Peer) *rpcscheduler.PeerPacket {
	parallelCount := config.DefaultClientParallelCount
	if config, ok := dynconfig.GetSchedulerClusterClientConfig(); ok && config.ParallelCount > 0 {
		parallelCount = int(config.ParallelCount)
	}

	var stealPeers []*rpcscheduler.PeerPacket_DestPeer
	for _, candidateParent := range candidateParents {
		stealPeers = append(stealPeers, &rpcscheduler.PeerPacket_DestPeer{
			Ip:      candidateParent.Host.IP,
			RpcPort: candidateParent.Host.Port,
			PeerId:  candidateParent.ID,
		})
	}

	return &rpcscheduler.PeerPacket{
		TaskId:        peer.Task.ID,
		SrcPid:        peer.ID,
		ParallelCount: int32(parallelCount),
		MainPeer: &rpcscheduler.PeerPacket_DestPeer{
			Ip:      parent.Host.IP,
			RpcPort: parent.Host.Port,
			PeerId:  parent.ID,
		},
		StealPeers: stealPeers,
		Code:       base.Code_Success,
	}
}
