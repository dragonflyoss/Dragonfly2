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

package basic

import (
	"sort"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator/basic"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/supervise"
)

const name = "basic"

func init() {
	scheduler.Register(newBasicSchedulerBuilder())
}

type basicSchedulerBuilder struct {
	name string
}

func newBasicSchedulerBuilder() scheduler.Builder {
	return &basicSchedulerBuilder{
		name: name,
	}
}

var _ scheduler.Builder = (*basicSchedulerBuilder)(nil)

func (builder *basicSchedulerBuilder) Build(cfg *config.SchedulerConfig, opts *scheduler.BuildOptions) (scheduler.Scheduler, error) {
	logger.Debugf("start create basic scheduler...")
	evalFactory := evaluator.NewEvaluatorFactory(cfg)
	evalFactory.Register("default", basic.NewEvaluator(cfg))
	evalFactory.RegisterGetEvaluatorFunc(0, func(taskID string) (string, bool) { return "default", true })
	sched := &Scheduler{
		evaluator:   evalFactory,
		peerManager: opts.PeerManager,
		cfg:         cfg,
	}
	logger.Debugf("create basic scheduler successfully")
	return sched, nil
}

func (builder *basicSchedulerBuilder) Name() string {
	return builder.name
}

type Scheduler struct {
	evaluator   evaluator.Evaluator
	peerManager supervise.PeerMgr
	cfg         *config.SchedulerConfig
}

func (s *Scheduler) ScheduleChildren(peer *supervise.Peer) (children []*supervise.Peer) {
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debug("start schedule children flow")
	if s.evaluator.IsBadNode(peer) {
		logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debug("stop schedule children flow because peer is bad node")
		return
	}
	freeUpload := peer.Host.GetFreeUploadLoad()
	candidateChildren := s.selectCandidateChildren(peer, freeUpload*2)
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("select num %d candidate children %v", len(candidateChildren), candidateChildren)
	evalResult := make(map[float64]*supervise.Peer)
	var evalScore []float64
	for _, child := range candidateChildren {
		score := s.evaluator.Evaluate(peer, child)
		evalResult[score] = child
		evalScore = append(evalScore, score)
	}
	sort.Float64s(evalScore)
	for i := range evalScore {
		if freeUpload <= 0 {
			break
		}
		child := evalResult[evalScore[len(evalScore)-i-1]]
		if child.GetParent() == peer {
			continue
		}
		children = append(children, child)
		freeUpload--
	}
	for _, child := range children {
		child.ReplaceParent(peer)
	}
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("final schedule children list %v", children)
	return
}

func (s *Scheduler) ScheduleParent(peer *supervise.Peer) (*supervise.Peer, []*supervise.Peer, bool) {
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debug("start schedule parent flow")
	//if !s.evaluator.NeedAdjustParent(peer) {
	//	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("stop schedule parent flow because peer is not need adjust parent", peer.PeerID)
	//	if peer.GetParent() == nil {
	//		return nil, nil, false
	//	}
	//	return peer.GetParent(), []*types.Peer{peer.GetParent()}, true
	//}
	candidateParents := s.selectCandidateParents(peer, s.cfg.CandidateParentCount)
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("select num %d candidates parents, current task tree node count %d ",
		len(candidateParents), peer.Task.ListPeers().Size())
	if len(candidateParents) == 0 {
		return nil, nil, false
	}
	evalResult := make(map[float64]*supervise.Peer)
	var evalScore []float64
	for _, candidate := range candidateParents {
		score := s.evaluator.Evaluate(candidate, peer)
		logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("evaluate score candidate %s is %f", candidate.PeerID, score)
		evalResult[score] = candidate
		evalScore = append(evalScore, score)
	}
	sort.Float64s(evalScore)
	var parents = make([]*supervise.Peer, 0, len(candidateParents))
	for i := range evalScore {
		parent := evalResult[evalScore[len(evalScore)-i-1]]
		parents = append(parents, parent)
	}
	if parents[0] != peer.GetParent() {
		peer.ReplaceParent(parents[0])
	}
	logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("primary parent %s is selected", parents[0].PeerID)
	return parents[0], parents[1:], true
}

func (s *Scheduler) selectCandidateChildren(peer *supervise.Peer, limit int) (list []*supervise.Peer) {
	return s.peerManager.Pick(peer.Task, limit, func(candidateNode *supervise.Peer) bool {
		if candidateNode == nil {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer is not selected because it is nil")
			return false
		}
		if candidateNode.IsDone() {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is not selected because it has done",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsLeave() {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is not selected because it has left",
				candidateNode.PeerID)
			return false
		}
		if candidateNode == peer {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is not selected because it and peer are the same",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsAncestorOf(peer) {
			logger.WithTaskAndPeerID(peer.Task.TaskID,
				peer.PeerID).Debugf("******candidate child peer %s is not selected because peer's ancestor is candidate peer", candidateNode.PeerID)
			return false
		}
		if candidateNode.GetFinishedNum() > peer.GetFinishedNum() {
			logger.WithTaskAndPeerID(peer.Task.TaskID,
				peer.PeerID).Debugf("******candidate child peer %s is not selected because it finished number of download is more than peer's",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.Host != nil && candidateNode.Host.CDN {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is not selected because it is a cdn host",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.GetParent() == nil {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is selected because it has not parent",
				candidateNode.PeerID)
			return true
		}

		if candidateNode.GetParent() != nil && s.evaluator.IsBadNode(candidateNode.GetParent()) {
			logger.WithTaskAndPeerID(peer.Task.TaskID,
				peer.PeerID).Debugf("******candidate child peer %s is selected because it has parent and parent status is not health", candidateNode.PeerID)
			return true
		}
		logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer %s is selected", candidateNode.PeerID)
		return false
	})
}

func (s *Scheduler) selectCandidateParents(peer *supervise.Peer, limit int) (list []*supervise.Peer) {
	if !peer.Task.CanSchedule() {
		logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++peer %s can not be scheduled because task status", peer.PeerID)
		return nil
	}
	return s.peerManager.PickReverse(peer.Task, limit, func(candidateNode *supervise.Peer) bool {
		if candidateNode == nil {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer is not selected because it is nil")
			return false
		}
		if s.evaluator.IsBadNode(candidateNode) {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it is badNode",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsLeave() {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it has already left",
				candidateNode.PeerID)
			return false
		}
		if candidateNode == peer {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it and peer are the same",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsDescendantOf(peer) {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it's ancestor is peer",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.Host.GetFreeUploadLoad() <= 0 {
			logger.WithTaskAndPeerID(peer.Task.TaskID,
				peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it's free upload load equal to less than zero",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsWaiting() {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it's status is waiting",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.GetFinishedNum() < peer.GetFinishedNum() {
			logger.WithTaskAndPeerID(peer.Task.TaskID,
				peer.PeerID).Debugf("++++++candidate parent peer %s is not selected because it finished number of download is smaller than peer's",
				candidateNode.PeerID)
			return false
		}
		logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer %s is selected", candidateNode.PeerID)
		return true
	})
}
