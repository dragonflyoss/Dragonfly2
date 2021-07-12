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

package core

import (
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type Scheduler struct {
	factory     *evaluatorFactory
	ascheduler  string
	bscheduler  string
	taskManager daemon.TaskMgr
}

func newScheduler(cfg *config.SchedulerConfig, taskManager daemon.TaskMgr) *Scheduler {
	evalFactory := newEvaluatorFactory(cfg)
	return &Scheduler{
		factory:     evalFactory,
		ascheduler:  cfg.AScheduler,
		bscheduler:  cfg.BScheduler,
		taskManager: taskManager,
	}
}

// ScheduleChildren scheduler children to a peer
func (s *Scheduler) ScheduleChildren(peer *types.PeerNode, limit int) (children []*types.PeerNode, err error) {
	eval := s.factory.get(peer.Task.TaskID)
	freeLoad := peer.Host.GetFreeUploadLoad()
	candidateChildren := eval.SelectCandidateChildren(peer, limit)
	evalResult := make(map[float64]*types.PeerNode)
	for _, child := range candidateChildren {
		evalResult[eval.Evaluate(peer, child)] = child
	}

	for freeLoad > 0 {
		var chosen *types.PeerNode
		var value float64

		if chosen == nil {
			break
		}
		if schedulerResult[chosen] == 0 {
			children = append(children, chosen)
			schedulerResult[chosen] = 1
			freeLoad--
		}
	}
	for _, child := range children {
		if child.Parent == peer {
			continue
		} else {
			child.DeleteParent()
		}
		child.Parent = peer
	}

	s.taskManager.PeerTask.Update(peer)
	return
}

// ScheduleParent schedule a parent to a peer
func (s *Scheduler) ScheduleParent(peer *types.PeerNode) (primary *types.PeerNode, secondary []*types.PeerNode, err error) {
	if !types.IsRunning(peer) {
		return
	}
	candidates := s.factory.get(peer.Task.TaskID).SelectCandidateParents(peer)
	var value float64
	for _, parent := range candidates {
		worth := s.factory.get(peer.Task.TaskID).Evaluate(parent, peer)

		// scheduler the same parent, worth reduce a half
		if peer.Parent != nil && peer.Parent.PeerID == parent.PeerID {
			worth = worth / 2.0
		}

		if worth > value {
			value = worth
			primary = parent
		}
	}
	if primary != nil {
		if primary == peer.Parent {
			return
		}
		peer.SetParent(primary, 1)
	}
	logger.Debugf("[%s][%s]SchedulerParent scheduler a empty parent", peer.Task.TaskID, peer.PeerID)

	return
}

func (s *Scheduler) ScheduleBadNode(peer *types.PeerNode) (adjustNodes []*types.PeerNode, err error) {
	logger.Debugf("[%s][%s]Scheduler bad node", peer.Task.TaskID, peer.PeerID)
	parent := peer.Parent
	if parent != nil {
		peer.ReplaceParent()
		s.ScheduleChildren(parent)
	}

	for _, child := range peer.Children {
		s.ScheduleParent(child)
		adjustNodes = append(adjustNodes, child)
	}

	s.ScheduleParent(peer)
	adjustNodes = append(adjustNodes, peer)

	for _, node := range adjustNodes {
		parentID := ""
		if node.GetParent() != nil {
			parentID = node.GetParent().GetPeerID()
		}
		logger.Debugf("[%s][%s]SchedulerBadNode [%s] scheduler a new parent [%s]", peer.GetTask().GetTaskID(), peer.GetPeerID(),
			node.GetPeerID(), parentID)
	}

	return
}

func (s *Scheduler) ScheduleLeaveNode(peer *types.PeerNode) (adjustNodes []*types.PeerNode, err error) {
	parent := peer.Parent
	if parent != nil {
		peer.DeleteParent()
		s.taskManager.PeerTask.Update(pNode)
	}
	s.taskManager.PeerTask.Update(peer)

	for _, child := range peer.Children {
		child.DeleteParent()
		s.ScheduleParent(child)
		adjustNodes = append(adjustNodes, child)
	}

	return
}

func (s *Scheduler) ScheduleAdjustParentNode(peer *types.PeerNode) (primary *types.PeerNode, secondary []*types.PeerNode, err error) {
	parent := peer.Parent
	if parent != nil {
		pNode := parent.DstPeerTask
		peer.DeleteParent()
		s.taskManager.PeerTask.Update(pNode)
	}
	return s.ScheduleParent(peer)
}

func (s *Scheduler) ScheduleDone(peer *types.PeerNode) (parent *types.PeerNode, err error) {
	parent = peer.Parent
	if parent == nil {
		return
	}
	peer.DeleteParent()
	s.taskManager.PeerTask.Update(parent)

	return
}
