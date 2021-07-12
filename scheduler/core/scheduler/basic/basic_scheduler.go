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
	"fmt"
	"sort"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator/basic"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

const name = "basic"

type basicSchedulerBuilder struct {
	name string
	cfg  *config.SchedulerConfig
}

func (builder *basicSchedulerBuilder) Build(opts scheduler.BuildOptions) (scheduler.Scheduler, error) {
	evalFactory := evaluator.NewEvaluatorFactory(builder.cfg)
	evalFactory.Register("default", basic.NewEvaluator())
	evalFactory.RegisterGetEvaluatorFunc(0, func(taskID string) (string, bool) { return "default", true })
	sch := &Scheduler{
		evaluator:   evalFactory,
		peerManager: opts.PeerManager,
	}
	return sch, nil
}

func (*basicSchedulerBuilder) Name() string {
	return name
}

type Scheduler struct {
	evaluator   evaluator.Evaluator
	peerManager daemon.PeerMgr
}

func (s *Scheduler) ScheduleChildren(peer *types.PeerNode) (children []*types.PeerNode) {
	logger.Debugf("[%s][%s]scheduler children", peer.Task.TaskID, peer.PeerID)
	if s.evaluator.IsBadNode(peer) {
		return
	}
	freeUpload := peer.Host.GetFreeUploadLoad()
	candidateChildren := s.selectCandidateChildren(peer, freeUpload*2)
	evalResult := make(map[float64]*types.PeerNode)
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
		if child.Parent == peer {
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

func (s *Scheduler) ScheduleParent(peer *types.PeerNode, limit int) (parent *types.PeerNode, candidateParents []*types.PeerNode) {
	logger.Debugf("[%s][%s]scheduler parent", peer.Task.TaskID, peer.PeerID)
	if !s.evaluator.NeedAdjustParent(peer) {
		return
	}
	candidateParents = s.selectCandidateParents(peer, limit)
	var value float64
	for _, candidate := range candidateParents {
		worth := s.evaluator.Evaluate(parent, peer)

		// scheduler the same parent, worth reduce a half
		if peer.Parent != nil && peer.Parent.PeerID == candidate.PeerID {
			worth = worth / 2.0
		}

		if worth > value {
			value = worth
			parent = candidate
		}
	}
	if parent == peer.Parent {
		return
	}
	peer.ReplaceParent(parent)
	return
}

func (s *Scheduler) IsBadNode(peer *types.PeerNode) bool {
	return s.evaluator.IsBadNode(peer)
}

func (s *Scheduler) selectCandidateChildren(peer *types.PeerNode, limit int) (list []*types.PeerNode) {
	if peer == nil || peer.Host.GetFreeUploadLoad() < 1 {
		return
	}
	return s.peerManager.Pick(peer.Task, limit, func(candidateNode *types.PeerNode) bool {
		// pick children
		if candidateNode == nil || candidateNode.Task.TaskID != peer.Task.TaskID {
			return false
		}
		if candidateNode.PeerID == peer.PeerID {
			return false
		} else if candidateNode.Success {
			return false
		} else if types.IsCDNHost(candidateNode.Host) {
			return false
		} else if peer.Parent != nil && peer.Parent == candidateNode {
			return false
		} else if candidateNode.IsAncestor(peer) || peer.IsAncestor(candidateNode) {
			return false
		} else if candidateNode.Parent != nil {
			return false
		}
		return true
	})
}

func (s *Scheduler) selectCandidateParents(peer *types.PeerNode, limit int) (list []*types.PeerNode) {
	if peer == nil {
		return
	}
	var msg []string
	list = s.peerManager.PickReverse(peer.Task, limit, func(candidateNode *types.PeerNode) bool {
		if candidateNode == nil {
			return false
		} else if peer.Task != candidateNode.Task {
			msg = append(msg, fmt.Sprintf("%s task[%s] not same", candidateNode.PeerID, candidateNode.Task.TaskID))
			return false
		} else if peer.PeerID == candidateNode.PeerID {
			return false
		} else if candidateNode.IsAncestor(peer) || peer.IsAncestor(candidateNode) {
			msg = append(msg, fmt.Sprintf("%s has relation", candidateNode.PeerID))
			return false
		} else if candidateNode.Host.GetFreeUploadLoad() < 1 {
			msg = append(msg, fmt.Sprintf("%s no load", candidateNode.PeerID))
			return false
		}
		if candidateNode.Success {
			return true
		}
		root := candidateNode.GetTreeRoot()
		if root != nil && root.Host != nil && types.IsCDNHost(root.Host) {
			return true
		}
		return true
	})
	if len(list) == 0 {
		logger.Debugf("[%s][%s] scheduler failed: \n%s", peer.Task.TaskID, peer.PeerID, strings.Join(msg, "\n"))
	}

	return
}
