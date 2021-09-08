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
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"k8s.io/apimachinery/pkg/util/sets"
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
	peerManager supervisor.PeerMgr
	cfg         *config.SchedulerConfig
}

func (s *Scheduler) ScheduleChildren(peer *supervisor.Peer, blankChildren sets.String) (children []*supervisor.Peer) {
	if s.evaluator.IsBadNode(peer) {
		peer.Log().Debug("terminate schedule children flow because peer is bad node")
		return
	}
	freeUpload := peer.Host.GetFreeUploadLoad()
	candidateChildren := s.selectCandidateChildren(peer, freeUpload*2, blankChildren)
	if len(candidateChildren) == 0 {
		return nil
	}
	evalResult := make(map[float64][]*supervisor.Peer)
	var evalScore []float64
	for _, child := range candidateChildren {
		score := s.evaluator.Evaluate(peer, child)
		evalResult[score] = append(evalResult[score], child)
		evalScore = append(evalScore, score)
	}
	sort.Float64s(evalScore)
	for i := range evalScore {
		if freeUpload <= 0 {
			break
		}
		peers := evalResult[evalScore[len(evalScore)-i-1]]
		for _, child := range peers {
			if freeUpload <= 0 {
				break
			}
			if child.GetParent() == peer {
				continue
			}
			children = append(children, child)
			freeUpload--
		}
	}
	for _, child := range children {
		child.ReplaceParent(peer)
	}
	peer.Log().Debugf("schedule children result: %v", children)
	return
}

func (s *Scheduler) ScheduleParent(peer *supervisor.Peer, blankParents sets.String) (*supervisor.Peer, []*supervisor.Peer, bool) {
	//if !s.evaluator.NeedAdjustParent(peer) {
	//	peer.Log().Debugf("stop schedule parent flow because peer is not need adjust parent", peer.PeerID)
	//	if peer.GetParent() == nil {
	//		return nil, nil, false
	//	}
	//	return peer.GetParent(), []*types.Peer{peer.GetParent()}, true
	//}
	candidateParents := s.selectCandidateParents(peer, s.cfg.CandidateParentCount, blankParents)
	if len(candidateParents) == 0 {
		return nil, nil, false
	}
	evalResult := make(map[float64][]*supervisor.Peer)
	var evalScore []float64
	for _, parent := range candidateParents {
		score := s.evaluator.Evaluate(parent, peer)
		peer.Log().Debugf("evaluate score candidate %s is %f", parent.PeerID, score)
		evalResult[score] = append(evalResult[score], parent)
		evalScore = append(evalScore, score)
	}
	sort.Float64s(evalScore)
	var parents = make([]*supervisor.Peer, 0, len(candidateParents))
	for i := range evalScore {
		parents = append(parents, evalResult[evalScore[len(evalScore)-i-1]]...)
	}
	if parents[0] != peer.GetParent() {
		peer.ReplaceParent(parents[0])
	}
	peer.Log().Debugf("primary parent %s is selected", parents[0].PeerID)
	return parents[0], parents[1:], true
}

func (s *Scheduler) selectCandidateChildren(peer *supervisor.Peer, limit int, blankChildren sets.String) (candidateChildren []*supervisor.Peer) {
	peer.Log().Debug("start schedule children flow")
	defer peer.Log().Debugf("finish schedule parent flow, select num %d candidate children, "+
		"current task tree node count %d, back source peers: %s", len(candidateChildren), peer.Task.ListPeers().Size(), peer.Task.GetBackSourcePeers())
	candidateChildren = peer.Task.Pick(limit, func(candidateNode *supervisor.Peer) bool {
		if candidateNode == nil {
			peer.Log().Debugf("******candidate child peer is not selected because it is nil******")
			return false
		}
		if blankChildren != nil && blankChildren.Has(candidateNode.PeerID) {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("******candidate child peer is not selected because it in blank children set******")
			return false
		}
		if candidateNode.IsDone() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it has done******", candidateNode.PeerID)
			return false
		}
		if candidateNode.IsLeave() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it has left******", candidateNode.PeerID)
			return false
		}
		if candidateNode.IsWaiting() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it's status is Waiting******", candidateNode.PeerID)
			return false
		}
		if candidateNode == peer {
			peer.Log().Debugf("******candidate child peer %s is not selected because it and peer are the same******", candidateNode.PeerID)
			return false
		}
		if candidateNode.IsAncestorOf(peer) {
			peer.Log().Debugf("******candidate child peer %s is not selected because peer's ancestor is candidate peer******", candidateNode.PeerID)
			return false
		}
		if candidateNode.GetFinishedNum() >= peer.GetFinishedNum() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it finished number of download is equal to or greater than peer's"+
				"******", candidateNode.PeerID)
			return false
		}
		if candidateNode.Host != nil && candidateNode.Host.CDN {
			peer.Log().Debugf("******candidate child peer %s is not selected because it is a cdn host******", candidateNode.PeerID)
			return false
		}
		if !candidateNode.IsConnected() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it is not connected******", candidateNode.PeerID)
			return false
		}
		if candidateNode.GetParent() == nil {
			peer.Log().Debugf("******[selected]candidate child peer %s is selected because it has not parent[selected]******", candidateNode.PeerID)
			return true
		}

		if candidateNode.GetParent() != nil && s.evaluator.IsBadNode(candidateNode.GetParent()) {
			peer.Log().Debugf("******[selected]candidate child peer %s is selected because parent's status is not health[selected]******",
				candidateNode.PeerID)
			return true
		}
		peer.Log().Debugf("******[default]candidate child peer %s is selected[default]******", candidateNode.PeerID)
		return true
	})
	return
}

func (s *Scheduler) selectCandidateParents(peer *supervisor.Peer, limit int, blankParents sets.String) (candidateParents []*supervisor.Peer) {
	peer.Log().Debug("start schedule parent flow")
	defer peer.Log().Debugf("finish schedule parent flow, select num %d candidates parents, "+
		"current task tree node count %d, back source peers: %s", len(candidateParents), peer.Task.ListPeers().Size(), peer.Task.GetBackSourcePeers())
	if !peer.Task.CanSchedule() {
		peer.Log().Debugf("++++++peer can not be scheduled because task cannot be scheduled at this time，waiting task status become seeding. "+
			"it current status is %s++++++", peer.Task.GetStatus())
		return nil
	}
	candidateParents = peer.Task.PickReverse(limit, func(candidateNode *supervisor.Peer) bool {
		if candidateNode == nil {
			peer.Log().Debugf("++++++candidate parent peer is not selected because it is nil++++++")
			return false
		}
		if blankParents != nil && blankParents.Has(candidateNode.PeerID) {
			logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Debugf("++++++candidate parent peer is not selected because it in blank parent set++++++")
			return false
		}
		if s.evaluator.IsBadNode(candidateNode) {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it is badNode++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsLeave() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it has already left++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode == peer {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it and peer are the same++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsDescendantOf(peer) {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's ancestor is peer++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.Host.GetFreeUploadLoad() <= 0 {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's free upload load equal to less than zero++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.IsWaiting() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's status is waiting++++++",
				candidateNode.PeerID)
			return false
		}
		if candidateNode.GetFinishedNum() <= peer.GetFinishedNum() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it finished number of download is equal to or smaller than peer's"+
				"++++++", candidateNode.PeerID)
			return false
		}
		peer.Log().Debugf("++++++[default]candidate parent peer %s is selected[default]", candidateNode.PeerID)
		return true
	})
	return
}
