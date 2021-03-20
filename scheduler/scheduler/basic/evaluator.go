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
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/types"
	"fmt"
	"strings"
	"time"
)

type Evaluator struct {
}

func NewEvaluator() *Evaluator {
	e := &Evaluator{}
	return e
}

func (e *Evaluator) NeedAdjustParent(peer *types.PeerTask) bool {
	parent := peer.GetParent()

	if parent == nil {
		return true
	}

	costHistory := parent.CostHistory
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := e.getAvgAndLastCost(parent.CostHistory, 4)
	if avgCost * 40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: node cost is too long", peer.Pid)
		return true
	}

	return (avgCost * 20) < lastCost
}

func (e *Evaluator) IsNodeBad(peer *types.PeerTask) (result bool) {
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

	if avgCost * 40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: node cost is too long", peer.Pid)
		return true
	}

	return false
}

func (e *Evaluator) getAvgAndLastCost(list []int32, splitPostition int) (avgCost, lastCost int32) {
	length := len(list)
	totalCost := int32(0)
	for i, cost := range list {
		totalCost += cost
		if length - i < splitPostition {
			lastCost += cost
		}
	}

	avgCost = totalCost / int32(length)
	lastCost = lastCost / int32(splitPostition)
	return
}

func (e *Evaluator) SelectChildCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
	if peer == nil {
		return
	}
	mgr.GetPeerTaskManager().Walker(func(pt *types.PeerTask) bool {
		if pt == nil || peer.Task != pt.Task {
			return true
		}
		if pt.Pid == peer.Pid {
			return true
		} else if pt.IsDown() {
			return true
		} else if pt.Success {
			return true
		} else if pt.Host.Type == types.HostTypeCdn{
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
		return true
	})
	return
}

func (e *Evaluator) SelectParentCandidates(peer *types.PeerTask) (list []*types.PeerTask) {
	if peer == nil {
		logger.Debugf("peerTask is nil")
		return
	}
	var msg []string
	mgr.GetPeerTaskManager().Walker(func(pt *types.PeerTask) bool {
		if pt == nil {
			return true
		} else if peer.Task != pt.Task {
			msg = append(msg, fmt.Sprintf("%s task[%s] not same", pt.Pid, pt.Task.TaskId))
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
		return true
	})
	if len(list) == 0 {
		logger.Debugf("[%s][%s] scheduler failed: \n%s", peer.Task.TaskId, peer.Pid, strings.Join(msg, "\n"))
	}

	return
}

func (e *Evaluator) Evaluate(dst *types.PeerTask, src *types.PeerTask) (result float64, error error) {
	profits := e.GetProfits(dst, src)

	load, err := e.GetHostLoad(dst.Host)
	if err != nil {
		return
	}

	dist, err := e.GetDistance(dst, src)
	if err != nil {
		return
	}

	result = profits * load * dist
	return
}

// GetProfits 0.0~unlimited larger and better
func (e *Evaluator) GetProfits(dst *types.PeerTask, src *types.PeerTask) float64 {
	diff := src.GetDiffPieceNum(dst)
	deep := dst.GetDeep()

	return float64((diff+1)*src.GetSubTreeNodesNum()) / float64(deep*deep)
}

// GetHostLoad 0.0~1.0 larger and better
func (e *Evaluator) GetHostLoad(host *types.Host) (load float64, err error) {
	load = 1.0 - host.GetUploadLoadPercent()
	return
}

// GetDistance 0.0~1.0 larger and better
func (e *Evaluator) GetDistance(dst *types.PeerTask, src *types.PeerTask) (dist float64, err error) {
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

