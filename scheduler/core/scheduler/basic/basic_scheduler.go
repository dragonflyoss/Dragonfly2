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
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
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
	logger.Debugf("start create basic scheduler")
	evalFactory := evaluator.NewEvaluatorFactory(cfg)
	evalFactory.Register("default", basic.NewEvaluator(cfg))
	evalFactory.RegisterGetEvaluatorFunc(0, func(taskID string) (string, bool) { return "default", true })
	sch := &Scheduler{
		evaluator:   evalFactory,
		peerManager: opts.PeerManager,
		cfg:         cfg,
	}
	logger.Debugf("create basic scheduler successfully")
	return sch, nil
}

func (builder *basicSchedulerBuilder) Name() string {
	return builder.name
}

type Scheduler struct {
	evaluator   evaluator.Evaluator
	peerManager daemon.PeerMgr
	cfg         *config.SchedulerConfig
}

func (s *Scheduler) ScheduleChildren(peer *types.Peer) (children []*types.Peer) {
	logger.Debugf("[%s][%s]scheduler children", peer.Task.TaskID, peer.PeerID)
	if s.evaluator.IsBadNode(peer) {
		return
	}
	freeUpload := peer.Host.GetFreeUploadLoad()
	candidateChildren := s.selectCandidateChildren(peer, freeUpload*2)
	evalResult := make(map[float64]*types.Peer)
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
	return
}

func (s *Scheduler) ScheduleParent(peer *types.Peer) (parent *types.Peer, candidateParents []*types.Peer) {
	logger.Debugf("[%s][%s]scheduler parent", peer.Task.TaskID, peer.PeerID)
	if !s.evaluator.NeedAdjustParent(peer) {
		return
	}
	candidateParents = s.selectCandidateParents(peer, s.cfg.CandidateParentCount)
	var value float64
	for _, candidate := range candidateParents {
		worth := s.evaluator.Evaluate(parent, peer)

		// scheduler the same parent, worth reduce a half
		if peer.GetParent() != nil && peer.GetParent().PeerID == candidate.PeerID {
			worth = worth / 2.0
		}

		if worth > value {
			value = worth
			parent = candidate
		}
	}
	if parent == peer.GetParent() {
		return
	}
	peer.ReplaceParent(parent)
	return
}

func (s *Scheduler) selectCandidateChildren(peer *types.Peer, limit int) (list []*types.Peer) {
	return s.peerManager.Pick(peer.Task, limit, func(candidateNode *types.Peer) bool {
		if candidateNode == nil || candidateNode.IsDone() || candidateNode.Host.CDN {
			return false
		}
		if candidateNode.GetParent() == nil {
			return true
		}
		if candidateNode.GetParent() != nil && s.evaluator.IsBadNode(candidateNode.GetParent()) {
			return true
		}
		return false
	})
}

func (s *Scheduler) selectCandidateParents(peer *types.Peer, limit int) (list []*types.Peer) {
	return s.peerManager.PickReverse(peer.Task, limit, func(candidateNode *types.Peer) bool {
		if candidateNode == nil || s.evaluator.IsBadNode(candidateNode) {
			return false
		}
		if candidateNode.IsLeave() {
			return false
		}
		if candidateNode.Host.GetFreeUploadLoad() > 0 {
			return true
		}
		return false
	})
}
