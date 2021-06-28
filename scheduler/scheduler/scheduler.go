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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/scheduler/base"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type Scheduler struct {
	evaluatorFactory *evaluatorFactory
	abtest           bool
	ascheduler       string
	bscheduler       string
	taskManager      daemon.TaskMgr
}

func New(cfg config.SchedulerConfig, taskManager daemon.TaskMgr) *Scheduler {
	ef := newEvaluatorFactory(cfg)
	ef.register("default", base.newEvaluator(taskManager))
	ef.registerGetEvaluatorFunc(0, func(*types.Task) (string, bool) { return "default", true })
	return &Scheduler{
		evaluatorFactory: ef,
		abtest:           cfg.ABTest,
		ascheduler:       cfg.AScheduler,
		bscheduler:       cfg.BScheduler,
		taskManager:      taskManager,
	}
}

// scheduler children to a peer
func (s *Scheduler) ScheduleChildren(peer *types.PeerTask) (children []*types.PeerTask, err error) {
	if peer == nil || peer.IsDown() {
		return
	}

	freeLoad := peer.GetFreeLoad()
	candidates := s.evaluatorFactory.get(peer.Task).selectChildCandidates(peer)
	schedulerResult := make(map[*types.PeerTask]int8)
	for freeLoad > 0 {
		var chosen *types.PeerTask
		value := 0.0
		for _, child := range candidates {
			val, _ := s.evaluatorFactory.get(peer.Task).evaluate(peer, child)
			if val > value && schedulerResult[child] == 0 {
				value = val
				chosen = child
			}
		}
		if chosen == nil {
			break
		}
		if schedulerResult[chosen] == 0 {
			children = append(children, chosen)
			schedulerResult[chosen]++
			freeLoad--
		}
	}
	for _, child := range children {
		if child.GetParent() != nil {
			if child.GetParent().DstPeerTask == peer {
				continue
			} else {
				child.DeleteParent()
			}
		}
		child.AddParent(peer, 1)
	}

	s.taskManager.PeerTask.Update(peer)
	return
}

// ScheduleParent schedule a parent to a peer
func (s *Scheduler) ScheduleParent(peer *types.PeerTask) (primary *types.PeerTask, secondary []*types.PeerTask, err error) {
	if peer == nil || peer.Success || peer.IsDown() {
		return
	}

	var oldParent *types.PeerTask
	if peer.GetParent() != nil && peer.GetParent().DstPeerTask != nil {
		oldParent = peer.GetParent().DstPeerTask
	}

	candidates := s.evaluatorFactory.get(peer.Task).selectParentCandidates(peer)
	value := 0.0
	for _, parent := range candidates {
		if parent == nil {
			continue
		}
		val, _ := s.evaluatorFactory.get(peer.Task).evaluate(parent, peer)

		// scheduler the same parent, value reduce a half
		if peer.GetParent() != nil && peer.GetParent().DstPeerTask != nil &&
			peer.GetParent().DstPeerTask.Pid == parent.Pid {
			val = val / 2.0
		}

		if val > value {
			value = val
			primary = parent
		}
	}
	if primary != nil {
		if primary == oldParent {
			return
		}
		peer.DeleteParent()
		peer.AddParent(primary, 1)
		s.taskManager.PeerTask.Update(primary)
		s.taskManager.PeerTask.Update(oldParent)
	}
	logger.Debugf("[%s][%s]SchedulerParent scheduler a empty parent", peer.Task.TaskID, peer.Pid)

	return
}

func (s *Scheduler) ScheduleBadNode(peer *types.PeerTask) (adjustNodes []*types.PeerTask, err error) {
	logger.Debugf("[%s][%s]SchedulerBadNode scheduler node is bad", peer.Task.TaskID, peer.Pid)
	parent := peer.GetParent()
	if parent != nil && parent.DstPeerTask != nil {
		pNode := parent.DstPeerTask
		peer.DeleteParent()
		s.taskManager.PeerTask.Update(pNode)
	}

	for _, child := range peer.GetChildren() {
		child.SrcPeerTask.DeleteParent()
		s.ScheduleParent(child.SrcPeerTask)
		adjustNodes = append(adjustNodes, child.SrcPeerTask)
	}

	s.ScheduleParent(peer)
	adjustNodes = append(adjustNodes, peer)

	for _, node := range adjustNodes {
		parentID := ""
		if node.GetParent() != nil {
			parentID = node.GetParent().DstPeerTask.Pid
		}
		logger.Debugf("[%s][%s]SchedulerBadNode [%s] scheduler a new parent [%s]", peer.Task.TaskID, peer.Pid,
			node.Pid, parentID)
	}

	return
}

func (s *Scheduler) ScheduleLeaveNode(peer *types.PeerTask) (adjustNodes []*types.PeerTask, err error) {
	parent := peer.GetParent()
	if parent != nil && parent.DstPeerTask != nil {
		pNode := parent.DstPeerTask
		peer.DeleteParent()
		peer.SetDown()
		s.taskManager.PeerTask.Update(pNode)
	}
	s.taskManager.PeerTask.Update(peer)

	for _, child := range peer.GetChildren() {
		child.SrcPeerTask.DeleteParent()
		s.ScheduleParent(child.SrcPeerTask)
		adjustNodes = append(adjustNodes, child.SrcPeerTask)
	}

	return
}

func (s *Scheduler) ScheduleAdjustParentNode(peer *types.PeerTask) (primary *types.PeerTask, secondary []*types.PeerTask, err error) {
	parent := peer.GetParent()
	if parent != nil && parent.DstPeerTask != nil {
		pNode := parent.DstPeerTask
		peer.DeleteParent()
		s.taskManager.PeerTask.Update(pNode)
	}
	return s.ScheduleParent(peer)
}

func (s *Scheduler) ScheduleDone(peer *types.PeerTask) (parent *types.PeerTask, err error) {
	if peer.GetParent() == nil {
		return
	}
	parent = peer.GetParent().DstPeerTask
	if parent == nil {
		return
	}
	peer.DeleteParent()
	s.taskManager.PeerTask.Update(parent)

	return
}

func (s *Scheduler) NeedAdjustParent(peer *types.PeerTask) bool {
	return s.evaluatorFactory.get(peer.Task).needAdjustParent(peer)
}

func (s *Scheduler) IsNodeBad(peer *types.PeerTask) bool {
	return s.evaluatorFactory.get(peer.Task).isNodeBad(peer)
}
