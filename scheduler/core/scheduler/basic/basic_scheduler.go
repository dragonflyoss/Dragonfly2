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

	"k8s.io/apimachinery/pkg/util/sets"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
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

func (builder *basicSchedulerBuilder) Build(cfg *config.SchedulerConfig, opts *scheduler.BuildOptions) (scheduler.Scheduler, error) {
	logger.Debugf("start create basic scheduler...")
	evaluator := evaluator.New(cfg.Algorithm, opts.PluginDir)
	sched := &Scheduler{
		evaluator:   evaluator,
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
	peerManager supervisor.PeerManager
	cfg         *config.SchedulerConfig
}

func (s *Scheduler) ScheduleChildren(peer *supervisor.Peer, blankChildren sets.String) (children []*supervisor.Peer) {
	if s.evaluator.IsBadNode(peer) {
		peer.Log().Debug("terminate schedule children flow because peer is bad node")
		return
	}
	freeUpload := peer.Host.GetFreeUploadLoad()
	candidateChildren := s.selectCandidateChildren(peer, int(freeUpload)*2, blankChildren)
	if len(candidateChildren) == 0 {
		return nil
	}
	evalResult := make(map[float64][]*supervisor.Peer)
	var evalScore []float64
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	for _, child := range candidateChildren {
		score := s.evaluator.Evaluate(peer, child, taskTotalPieceCount)
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
			if parent, ok := child.GetParent(); ok && parent == peer {
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
	candidateParents := s.selectCandidateParents(peer, s.cfg.CandidateParentCount, blankParents)
	if len(candidateParents) == 0 {
		return nil, nil, false
	}
	evalResult := make(map[float64][]*supervisor.Peer)
	var evalScore []float64
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	for _, parent := range candidateParents {
		score := s.evaluator.Evaluate(parent, peer, taskTotalPieceCount)
		peer.Log().Debugf("evaluate score candidate %s is %f", parent.ID, score)
		evalResult[score] = append(evalResult[score], parent)
		evalScore = append(evalScore, score)
	}
	sort.Float64s(evalScore)
	var parents = make([]*supervisor.Peer, 0, len(candidateParents))
	for i := range evalScore {
		parents = append(parents, evalResult[evalScore[len(evalScore)-i-1]]...)
	}

	if parent, ok := peer.GetParent(); ok && parents[0] != parent {
		peer.ReplaceParent(parents[0])
	}

	peer.Log().Debugf("primary parent %s is selected", parents[0].ID)
	return parents[0], parents[1:], true
}

func (s *Scheduler) selectCandidateChildren(peer *supervisor.Peer, limit int, blankChildren sets.String) (candidateChildren []*supervisor.Peer) {
	peer.Log().Debug("start schedule children flow")
	defer peer.Log().Debugf("finish schedule children flow, select num %d candidate children, "+
		"current task tree node count %d, back source peers: %v", len(candidateChildren), peer.Task.GetPeers().Len(), peer.Task.GetBackToSourcePeers())
	candidateChildren = peer.Task.Pick(limit, func(candidateNode *supervisor.Peer) bool {
		if candidateNode == nil {
			peer.Log().Debugf("******candidate child peer is not selected because it is nil******")
			return false
		}

		if blankChildren != nil && blankChildren.Has(candidateNode.ID) {
			logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Debugf("******candidate child peer is not selected because it in blank children set******")
			return false
		}

		if candidateNode.IsDone() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it has done******", candidateNode.ID)
			return false
		}

		if candidateNode.IsLeave() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it has left******", candidateNode.ID)
			return false
		}

		if candidateNode.IsWaiting() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it's status is Waiting******", candidateNode.ID)
			return false
		}

		if candidateNode == peer {
			peer.Log().Debugf("******candidate child peer %s is not selected because it and peer are the same******", candidateNode.ID)
			return false
		}

		if candidateNode.IsAncestor(peer) {
			peer.Log().Debugf("******candidate child peer %s is not selected because peer's ancestor is candidate peer******", candidateNode.ID)
			return false
		}

		if candidateNode.TotalPieceCount.Load() >= peer.TotalPieceCount.Load() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it finished number of download is equal to or greater than peer's"+
				"******", candidateNode.ID)
			return false
		}

		if candidateNode.Host != nil && candidateNode.Host.IsCDN {
			peer.Log().Debugf("******candidate child peer %s is not selected because it is a cdn host******", candidateNode.ID)
			return false
		}

		if !candidateNode.IsConnected() {
			peer.Log().Debugf("******candidate child peer %s is not selected because it is not connected******", candidateNode.ID)
			return false
		}

		if _, ok := candidateNode.GetParent(); !ok {
			peer.Log().Debugf("******[selected]candidate child peer %s is selected because it has not parent[selected]******", candidateNode.ID)
			return true
		}

		if parent, ok := candidateNode.GetParent(); ok && s.evaluator.IsBadNode(parent) {
			peer.Log().Debugf("******[selected]candidate child peer %s is selected because parent's status is not health[selected]******",
				candidateNode.ID)
			return true
		}

		peer.Log().Debugf("******[default]candidate child peer %s is selected[default]******", candidateNode.ID)
		return true
	})
	return
}

func (s *Scheduler) selectCandidateParents(peer *supervisor.Peer, limit int, blankParents sets.String) (candidateParents []*supervisor.Peer) {
	peer.Log().Debug("start schedule parent flow")
	defer peer.Log().Debugf("finish schedule parent flow, select num %d candidates parents, "+
		"current task tree node count %d, back source peers: %v", len(candidateParents), peer.Task.GetPeers().Len(), peer.Task.GetBackToSourcePeers())
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
		if blankParents != nil && blankParents.Has(candidateNode.ID) {
			logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Debugf("++++++candidate parent peer is not selected because it in blank parent set++++++")
			return false
		}
		if s.evaluator.IsBadNode(candidateNode) {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it is badNode++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode.IsLeave() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it has already left++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode == peer {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it and peer are the same++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode.IsDescendant(peer) {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's ancestor is peer++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode.Host.GetFreeUploadLoad() <= 0 {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's free upload load equal to less than zero++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode.IsWaiting() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it's status is waiting++++++",
				candidateNode.ID)
			return false
		}
		if candidateNode.TotalPieceCount.Load() <= peer.TotalPieceCount.Load() {
			peer.Log().Debugf("++++++candidate parent peer %s is not selected because it finished number of download is equal to or smaller than peer's"+
				"++++++", candidateNode.ID)
			return false
		}
		peer.Log().Debugf("++++++[default]candidate parent peer %s is selected[default]", candidateNode.ID)
		return true
	})
	return
}
