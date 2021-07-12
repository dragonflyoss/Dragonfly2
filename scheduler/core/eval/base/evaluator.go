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
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

const name = "basic"

func init() {
	core.Register(&basicEvaluatorBuilder{})
}

type basicEvaluatorBuilder struct{}

func (*basicEvaluatorBuilder) Build(opts core.BuildOptions) (core.Evaluator, error) {
	r := &evaluator{
		peerManager: opts.PeerManager,
	}
	return r, nil
}

func (*basicEvaluatorBuilder) Name() string {
	return name
}

type evaluator struct {
	peerManager daemon.PeerMgr
}

func newEvaluator(taskMgr daemon.TaskMgr) core.Evaluator {
	return &evaluator{}
}

var _ core.Evaluator = (*evaluator)(nil)

func (eval *evaluator) NeedAdjustParent(peer *types.PeerNode) bool {
	parent := peer.Parent

	if parent == nil {
		return true
	}

	costHistory := peer.CostHistory
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := eval.getAvgAndLastCost(parent.CostHistory, 4)
	if avgCost*40 < lastCost {
		logger.Debugf("IsBadNode [%s]: recent pieces have taken too long to download", peer.PeerID)
		return true
	}

	// todo adjust policy
	return (avgCost * 20) < lastCost
}

func (eval *evaluator) IsBadNode(peer *types.PeerNode) bool {
	parent := peer.Parent

	if parent == nil {
		return false
	}

	if peer.IsWaiting() {
		return false
	}

	lastActiveTime := peer.LastAccessTime

	if time.Now().After(lastActiveTime.Add(5 * time.Second)) {
		logger.Debugf("IsBadNode [%s]: node is expired", peer.PeerID)
		return true
	}

	costHistory := parent.CostHistory
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := eval.getAvgAndLastCost(costHistory, 4)

	if avgCost*40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: recent pieces have taken too long to download avg[%d] last[%d]", peer.PeerID, avgCost, lastCost)
		return true
	}

	return false
}

func (eval *evaluator) getAvgAndLastCost(list []int, splitPos int) (avgCost, lastCost int) {
	length := len(list)
	totalCost := 0
	for i, cost := range list {
		totalCost += cost
		if length-i <= splitPos {
			lastCost += cost
		}
	}
	avgCost = totalCost / length
	lastCost = lastCost / splitPos
	return
}

func (eval *evaluator) SelectCandidateChildren(peer *types.PeerNode, limit int) (list []*types.PeerNode) {
	if peer == nil || peer.Host.GetFreeUploadLoad() < 1 {
		return
	}
	return eval.peerManager.Pick(peer.Task.TaskID, limit, func(candidateNode *types.PeerNode) bool {
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

func (eval *evaluator) SelectCandidateParents(peer *types.PeerNode, limit int) (list []*types.PeerNode) {
	if peer == nil {
		return
	}
	var msg []string
	list = eval.peerManager.PickReverse(peer.Task.TaskID, limit, func(candidateNode *types.PeerNode) bool {
		if candidateNode == nil {
			return false
		} else if peer.Task != candidateNode.Task {
			msg = append(msg, fmt.Sprintf("%s task[%s] not same", candidateNode.PeerID, candidateNode.Task.TaskID))
			return false
		} else if peer.PeerID == peer.PeerID {
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
		} else {
			root := candidateNode.GetTreeRoot()
			if root != nil && root.Host != nil && types.IsCDNHost(root.Host) {
				return true
			}
		}
		return true
	})
	if len(list) == 0 {
		logger.Debugf("[%s][%s] scheduler failed: \n%s", peer.Task.TaskID, peer.PeerID, strings.Join(msg, "\n"))
	}

	return
}

// The bigger the better
func (eval *evaluator) Evaluate(dst *types.PeerNode, src *types.PeerNode) float64 {
	profits := eval.getProfits(dst, src)

	load := eval.getHostLoad(dst.Host)

	dist := eval.getDistance(dst, src)

	return profits * load * dist
}

// getProfits 0.0~unlimited larger and better
func (eval *evaluator) getProfits(dst *types.PeerNode, src *types.PeerNode) float64 {
	diff := types.GetDiffPieceNum(src, dst)
	depth := dst.GetDepth()

	return float64(int(diff+1)*src.GetWholeTreeNode()) / float64(depth*depth)
}

// getHostLoad 0.0~1.0 larger and better
func (eval *evaluator) getHostLoad(host *types.NodeHost) float64 {
	return 1.0 - host.GetUploadLoadPercent()
}

// getDistance 0.0~1.0 larger and better
func (eval *evaluator) getDistance(dst *types.PeerNode, src *types.PeerNode) float64 {
	hostDist := 40.0
	if dst.Host == src.Host {
		hostDist = 0.0
	} else {
		if src.Host.NetTopology != "" && dst.Host.NetTopology == src.Host.NetTopology {
			hostDist = 10.0
		} else if src.Host.IDC != "" && dst.Host.IDC == src.Host.IDC {
			hostDist = 20.0
		} else if dst.Host.SecurityDomain != src.Host.SecurityDomain {
			hostDist = 80.0
		}
	}
	return 1.0 - hostDist/80.0
}
