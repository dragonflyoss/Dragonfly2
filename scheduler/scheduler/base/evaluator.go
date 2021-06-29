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

package base

import (
	"fmt"
	"strings"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type evaluator struct {
	taskManager daemon.TaskMgr
}

var _ scheduler.Evaluator = (*evaluator)(nil)

func newEvaluator(taskMgr daemon.TaskMgr) scheduler.Evaluator {
	return &evaluator{taskManager: taskMgr}
}

func (eval *evaluator) NeedAdjustParent(peer *types.PeerNode) bool {
	parent := peer.Parent()

	if parent == nil {
		return true
	}

	costHistory := parent.CostHistory
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := e.getAvgAndLastCost(parent.CostHistory, 4)
	if avgCost*40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: node cost is too long", peer.Pid)
		return true
	}

	return (avgCost * 20) < lastCost
}

func (eval *evaluator) IsBadNode(peer *types.PeerNode) bool {
	parent := peer.GetParent()

	if parent == nil {
		return false
	}

	if peer.IsWaiting() {
		return false
	}

	lastActiveTime := peer.LastActiveTime

	if time.Now().After(lastActiveTime.Add(5 * time.Second)) {
		logger.Debugf("IsBadNode [%s]: node is expired", peer.Pid)
		return true
	}

	costHistory := parent.CostHistory
	if int32(len(costHistory)) < 4 {
		return false
	}

	avgCost, lastCost := e.getAvgAndLastCost(costHistory, 4)

	if avgCost*40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: node cost is too long avg[%d] last[%d]", peer.Pid, avgCost, lastCost)
		return true
	}

	return false
}

func (eval *evaluator) getAvgAndLastCost(list []int64, splitPos int) (avgCost, lastCost int64) {
	length := len(list)
	totalCost := int64(0)
	for i, cost := range list {
		totalCost += int64(cost)
		if length-i <= splitPos {
			lastCost += int64(cost)
		}
	}

	avgCost = totalCost / int64(length)
	lastCost = lastCost / int64(splitPos)
	return
}

func (eval *evaluator) SelectChildCandidates(peer *types.PeerNode) (list []*types.PeerNode) {
	if peer == nil {
		return
	}
	e.taskManager.PeerTask.Walker(peer.Task, -1, func(pt *types.PeerTask) bool {
		if pt == nil || peer.Task != pt.Task {
			return true
		}
		if pt.Pid == peer.Pid {
			return true
		} else if pt.IsDown() {
			return true
		} else if pt.Success {
			return true
		} else if pt.Host.Type == types.HostTypeCdn {
			return true
		} else if peer.GetParent() != nil && peer.GetParent().DstPeerTask == pt {
			return true
		} else if peer.GetFreeLoad() < 1 {
			return true
		} else if pt.IsAncestor(peer) || peer.IsAncestor(pt) {
			return true
		} else if pt.GetParent() != nil {
			return true
		}
		list = append(list, pt)
		if len(list) > 10 {
			return false
		}
		return true
	})
	return
}

func (eval *evaluator) SelectParentCandidates(peer *types.PeerNode) (list []*types.PeerNode) {
	if peer == nil {
		logger.Debugf("peerTask is nil")
		return
	}
	var msg []string
	e.taskManager.PeerTask.WalkerReverse(peer.Task, -1, func(pt *types.PeerTask) bool {
		if pt == nil {
			return true
		} else if peer.Task != pt.Task {
			msg = append(msg, fmt.Sprintf("%s task[%s] not same", pt.Pid, pt.Task.TaskID))
			return true
		} else if pt.IsDown() {
			msg = append(msg, fmt.Sprintf("%s is down", pt.Pid))
			return true
		} else if pt.Pid == peer.Pid {
			return true
		} else if pt.IsAncestor(peer) || peer.IsAncestor(pt) {
			msg = append(msg, fmt.Sprintf("%s has relation", pt.Pid))
			return true
		} else if pt.GetFreeLoad() < 1 {
			msg = append(msg, fmt.Sprintf("%s no load", pt.Pid))
			return true
		}
		if pt.Success {
			list = append(list, pt)
		} else {
			root := pt.GetRoot()
			if root != nil && root.Host != nil && root.Host.Type == types.HostTypeCdn {
				list = append(list, pt)
			} else {
				msg = append(msg, fmt.Sprintf("%s not finished and root is not cdn", pt.Pid))
			}
		}
		if len(list) > 10 {
			return false
		}
		return true
	})
	if len(list) == 0 {
		logger.Debugf("[%s][%s] scheduler failed: \n%s", peer.Task.TaskID, peer.Pid, strings.Join(msg, "\n"))
	}

	return
}

func (eval *evaluator) Evaluate(dst *types.PeerNode, src *types.PeerNode) float64 {
	profits := eval.getProfits(dst, src)

	load := eval.getHostLoad(dst.GetHost())

	dist := eval.getDistance(dst, src)

	return profits * load * dist
}

// getProfits 0.0~unlimited larger and better
func (eval *evaluator) getProfits(dst *types.PeerNode, src *types.PeerNode) float64 {
	diff := types.GetDiffPieceNum(src, dst)
	depth := dst.GetDepth()

	return float64((diff+1)*src.GetWholeTreeNode()) / float64(depth*depth)
}

// getHostLoad 0.0~1.0 larger and better
func (eval *evaluator) getHostLoad(host *types.Host) float64 {
	return 1.0 - host.GetUploadLoadPercent()
}

// getDistance 0.0~1.0 larger and better
func (eval *evaluator) getDistance(dst *types.PeerNode, src *types.PeerNode) float64 {
	hostDist := 40.0
	if dst.GetHost() == src.GetHost() {
		hostDist = 0.0
	} else {
		if src.GetHost().NetTopology != "" && dst.GetHost().NetTopology == src.GetHost().NetTopology {
			hostDist = 10.0
		} else if src.GetHost().Idc != "" && dst.GetHost().Idc == src.GetHost().Idc {
			hostDist = 20.0
		} else if dst.GetHost().SecurityDomain != src.GetHost().SecurityDomain {
			hostDist = 80.0
		}
	}
	return 1.0 - hostDist/80.0
}
