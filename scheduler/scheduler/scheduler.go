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

type Scheduler interface {
	// ScheduleParent schedule a parent and candidates to a peer.
	ScheduleParent(context.Context, *resource.Peer, set.SafeSet)

	// Find the parent that best matches the evaluation and notify peer.
	NotifyAndFindParent(context.Context, *resource.Peer, set.SafeSet) ([]*resource.Peer, bool)

	// Find the parent that best matches the evaluation.
	FindParent(context.Context, *resource.Peer, set.SafeSet) (*resource.Peer, bool)
}

type scheduler struct {
	// Evaluator interface.
	evaluator evaluator.Evaluator

	// Scheduler configuration.
	config *config.SchedulerConfig

	// Scheduler dynamic configuration.
	dynconfig config.DynconfigInterface
}

func New(cfg *config.SchedulerConfig, dynconfig config.DynconfigInterface, pluginDir string) Scheduler {
	return &scheduler{
		evaluator: evaluator.New(cfg.Algorithm, pluginDir),
		config:    cfg,
		dynconfig: dynconfig,
	}
}

// ScheduleParent schedule a parent and candidates to a peer.
func (s *scheduler) ScheduleParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) {
	var n int
	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return
		default:
		}

		// If the scheduling exceeds the RetryBackSourceLimit or peer needs back-to-source,
		// peer will download the task back-to-source.
		needBackToSource := peer.NeedBackToSource.Load()
		if (n >= s.config.RetryBackSourceLimit || needBackToSource) &&
			peer.Task.CanBackToSource() {
			stream, ok := peer.LoadStream()
			if !ok {
				peer.Log.Error("load stream failed")
				return
			}

			peer.Log.Infof("peer downloads back-to-source, scheduling %d times, peer need back-to-source %t",
				n, needBackToSource)

			// Notify peer back-to-source.
			if err := stream.Send(&rpcscheduler.PeerPacket{Code: base.Code_SchedNeedBackSource}); err != nil {
				peer.Log.Errorf("send packet failed: %s", err.Error())
				return
			}

			if err := peer.FSM.Event(resource.PeerEventDownloadFromBackToSource); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return
			}

			// If the task state is TaskStateFailed,
			// peer back-to-source and reset task state to TaskStateRunning.
			if peer.Task.FSM.Is(resource.TaskStateFailed) {
				if err := peer.Task.FSM.Event(resource.TaskEventDownload); err != nil {
					peer.Task.Log.Errorf("task fsm event failed: %s", err.Error())
					return
				}
			}

			return
		}

		// Handle peer schedule failed.
		if n >= s.config.RetryLimit {
			stream, ok := peer.LoadStream()
			if !ok {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer schedule failed.
			if err := stream.Send(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}); err != nil {
				peer.Log.Errorf("send packet failed: %s", err.Error())
				return
			}
			peer.Log.Errorf("peer scheduling exceeds the limit %d times and return code %d", s.config.RetryLimit, base.Code_SchedTaskStatusError)
			return
		}

		if _, ok := s.NotifyAndFindParent(ctx, peer, blocklist); !ok {
			n++
			peer.Log.Infof("schedule parent %d times failed", n)

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		peer.Log.Infof("schedule parent %d times successfully", n+1)
		return
	}
}

// NotifyAndFindParent finds parent that best matches the evaluation and notify peer.
func (s *scheduler) NotifyAndFindParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) ([]*resource.Peer, bool) {
	// Only PeerStateRunning peers need to be rescheduled,
	// and other states including the PeerStateBackToSource indicate that
	// they have been scheduled.
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer state is %s, can not schedule parent", peer.FSM.Current())
		return []*resource.Peer{}, false
	}

	// Find the candidate parent that can be scheduled.
	candidateParents := s.filterCandidateParents(peer, blocklist)
	if len(candidateParents) == 0 {
		peer.Log.Info("can not find candidate parents")
		return []*resource.Peer{}, false
	}

	// Sort candidate parents by evaluation score.
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		candidateParents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(candidateParents[i], peer, taskTotalPieceCount) > s.evaluator.Evaluate(candidateParents[j], peer, taskTotalPieceCount)
		},
	)

	// Delete inedges of vertex.
	if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
		peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())
		return []*resource.Peer{}, false
	}

	// Add edges between candidate parent and peer.
	var (
		parents   []*resource.Peer
		parentIDs []string
	)
	for _, candidateParent := range candidateParents {
		if err := peer.Task.AddPeerEdge(candidateParent, peer); err != nil {
			peer.Log.Debugf("peer adds edge failed: %s", err.Error())
			continue
		}
		parents = append(parents, candidateParent)
		parentIDs = append(parentIDs, candidateParent.ID)
	}

	if len(parents) <= 0 {
		peer.Log.Info("can not add edges for vertex")
		return []*resource.Peer{}, false
	}

	// Send scheduling success message.
	stream, ok := peer.LoadStream()
	if !ok {
		peer.Log.Error("load peer stream failed")
		return []*resource.Peer{}, false
	}

	if err := stream.Send(constructSuccessPeerPacket(s.dynconfig, peer, parents[0], parents[1:])); err != nil {
		peer.Log.Error(err)
		return []*resource.Peer{}, false
	}

	peer.Log.Infof("schedule parent successful, replace parent to %s and steal parents is %v",
		parentIDs[0], parentIDs[1:])
	return candidateParents, true
}

// FindParent finds parent that best matches the evaluation.
func (s *scheduler) FindParent(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet) (*resource.Peer, bool) {
	// Filter the candidate parent that can be scheduled.
	candidateParents := s.filterCandidateParents(peer, blocklist)
	if len(candidateParents) == 0 {
		peer.Log.Info("can not find candidate parents")
		return nil, false
	}

	// Sort candidate parents by evaluation score.
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		candidateParents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(candidateParents[i], peer, taskTotalPieceCount) > s.evaluator.Evaluate(candidateParents[j], peer, taskTotalPieceCount)
		},
	)

	peer.Log.Infof("find parent %s successful", candidateParents[0].ID)
	return candidateParents[0], true
}

// Filter the candidate parent that can be scheduled.
func (s *scheduler) filterCandidateParents(peer *resource.Peer, blocklist set.SafeSet) []*resource.Peer {
	filterParentLimit := config.DefaultSchedulerFilterParentLimit
	if config, ok := s.dynconfig.GetSchedulerClusterConfig(); ok && filterParentLimit > 0 {
		filterParentLimit = int(config.FilterParentLimit)
	}

	var (
		candidateParents   []*resource.Peer
		candidateParentIDs []string
		n                  int
	)
	for _, vertex := range peer.Task.DAG.Vertices() {
		if n > filterParentLimit {
			break
		}
		n++

		value := vertex.Value
		if value == nil {
			continue
		}

		candidateParent, ok := value.(*resource.Peer)
		if !ok {
			continue
		}

		// Candidate parent is in blocklist.
		if blocklist.Contains(candidateParent.ID) {
			peer.Log.Debugf("candidate parent %s is not selected because it is in blocklist", candidateParent.ID)
			continue
		}

		// Candidate parent is itself.
		if candidateParent.ID == peer.ID {
			peer.Log.Debug("candidate parent is not selected because it is same")
			continue
		}

		// Candidate parent is bad node.
		if s.evaluator.IsBadNode(candidateParent) {
			peer.Log.Debugf("candidate parent %s is not selected because it is bad node", candidateParent.ID)
			continue
		}

		// Candidate parent can not find in dag.
		vertex, err := candidateParent.Task.DAG.GetVertex(candidateParent.ID)
		if err != nil {
			peer.Log.Debugf("can not find candidate parent %s vertex in dag", candidateParent.ID)
			continue
		}

		// Conditions for candidate parent to be a parent:
		// 1. parent has parent.
		// 2. parent has been back-to-source.
		// 3. parent has been succeeded.
		// 4. parent is seed peer.
		inDegree := vertex.InDegree()
		isBackToSource := candidateParent.IsBackToSource.Load()
		if candidateParent.Host.Type == resource.HostTypeNormal && inDegree == 0 && !isBackToSource &&
			!candidateParent.FSM.Is(resource.PeerStateSucceeded) {
			peer.Log.Debugf("candidate parent %s is not selected, because its download state is %t %d %t %s",
				candidateParent.ID, inDegree, int(candidateParent.Host.Type), isBackToSource, candidateParent.FSM.Current())
			continue
		}

		// Candidate parent's free upload is empty.
		if candidateParent.Host.FreeUploadLoad() <= 0 {
			peer.Log.Debugf("candidate parent %s is not selected because its free upload is empty, upload limit is %d, upload peer count is %d",
				candidateParent.ID, candidateParent.Host.UploadLoadLimit.Load(), candidateParent.Host.UploadPeerCount.Load())
			continue
		}

		candidateParents = append(candidateParents, candidateParent)
		candidateParentIDs = append(candidateParentIDs, candidateParent.ID)
	}

	peer.Log.Infof("candidate parents include %#v", candidateParentIDs)
	return candidateParents
}

// Construct peer successful packet.
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
