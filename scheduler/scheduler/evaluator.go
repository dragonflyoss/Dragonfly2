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
	"fmt"
	"strings"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type evaluatorOption func(*evaluator) *evaluator

type Evaluator interface {
	needAdjustParent(peer *types.PeerTask) bool
	isNodeBad(peer *types.PeerTask) bool
	evaluate(dst *types.PeerTask, src *types.PeerTask) (float64, error)
	selectChildCandidates(peer *types.PeerTask) []*types.PeerTask
	selectParentCandidates(peer *types.PeerTask) []*types.PeerTask
}

type evaluator struct {
	taskManager *manager.TaskManager
}

var _ Evaluator = (*evaluator)(nil)

// WithTaskManager sets task manager.
func withTaskManager(t *manager.TaskManager) evaluatorOption {
	return func(e *evaluator) *evaluator {
		e.taskManager = t
		return e
	}
}

func newEvaluator(options ...evaluatorOption) Evaluator {
	return newEvaluatorWithOptions(options...)
}

// NewEvaluatorWithOptions constructs a new instance of a Evaluator with additional options.
func newEvaluatorWithOptions(options ...evaluatorOption) Evaluator {
	evaluator := &evaluator{}

	// Apply all options
	for _, opt := range options {
		evaluator = opt(evaluator)
	}

	return evaluator
}

func (e *evaluator) needAdjustParent(peer *types.PeerTask) bool {
	parent := peer.GetParent()

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

func (e *evaluator) isNodeBad(peer *types.PeerTask) (result bool) {
	if peer.IsDown() {
		logger.Debugf("IsNodeBad [%s]: node is down ", peer.Pid)
		return true
	}

	parent := peer.GetParent()

	if parent == nil {
		return false
	}

	if peer.IsWaiting() {
		return false
	}

	lastActiveTime := peer.GetLastActiveTime()
	expired := time.Unix(lastActiveTime/int64(time.Second), lastActiveTime%int64(time.Second)).
		Add(time.Second * 5)
	if time.Now().After(expired) {
		logger.Debugf("IsNodeBad [%s]: node is expired", peer.Pid)
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

func (e *evaluator) getAvgAndLastCost(list []int64, splitPostition int) (avgCost, lastCost int64) {
	length := len(list)
	totalCost := int64(0)
	for i, cost := range list {
		totalCost += int64(cost)
		if length-i <= splitPostition {
			lastCost += int64(cost)
		}
	}

	avgCost = totalCost / int64(length)
	lastCost = lastCost / int64(splitPostition)
	return
}

func (e *evaluator) selectChildCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
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

func (e *evaluator) selectParentCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
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

func (e *evaluator) evaluate(dst *types.PeerTask, src *types.PeerTask) (result float64, error error) {
	profits := e.getProfits(dst, src)

	load, err := e.getHostLoad(dst.Host)
	if err != nil {
		return
	}

	dist, err := e.getDistance(dst, src)
	if err != nil {
		return
	}

	result = profits * load * dist
	return
}

// GetProfits 0.0~unlimited larger and better
func (e *evaluator) getProfits(dst *types.PeerTask, src *types.PeerTask) float64 {
	diff := src.GetDiffPieceNum(dst)
	deep := dst.GetDeep()

	return float64((diff+1)*src.GetSubTreeNodesNum()) / float64(deep*deep)
}

// GetHostLoad 0.0~1.0 larger and better
func (e *evaluator) getHostLoad(host *types.Host) (load float64, err error) {
	load = 1.0 - host.GetUploadLoadPercent()
	return
}

// GetDistance 0.0~1.0 larger and better
func (e *evaluator) getDistance(dst *types.PeerTask, src *types.PeerTask) (dist float64, err error) {
	hostDist := 40.0
	if dst.Host == src.Host {
		hostDist = 0.0
	} else if dst.Host != nil && src.Host != nil {
		if dst.Host.NetTopology == src.Host.NetTopology && src.Host.NetTopology != "" {
			hostDist = 10.0
		} else if dst.Host.Idc == src.Host.Idc && src.Host.Idc != "" {
			hostDist = 20.0
		} else if dst.Host.SecurityDomain != src.Host.SecurityDomain {
			hostDist = 80.0
		}
	}

	return 1.0 - hostDist/80.0, nil
}
