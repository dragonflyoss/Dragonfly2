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
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/types/host"
	"d7y.io/dragonfly/v2/scheduler/types/peer"
)

type baseEvaluator struct {
}

func NewEvaluator() evaluator.Evaluator {
	return &baseEvaluator{}
}

func (eval *baseEvaluator) NeedAdjustParent(peer *peer.PeerNode) bool {
	parent := peer.Parent

	if parent == nil {
		return true
	}

	costHistory := peer.CostHistory
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := getAvgAndLastCost(parent.CostHistory, 4)
	if avgCost*40 < lastCost {
		logger.Debugf("IsBadNode [%s]: recent pieces have taken too long to download", peer.PeerID)
		return true
	}

	// todo adjust policy
	return (avgCost * 20) < lastCost
}

func (eval *baseEvaluator) IsBadNode(peer *peer.PeerNode) bool {
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

	avgCost, lastCost := getAvgAndLastCost(costHistory, 4)

	if avgCost*40 < lastCost {
		logger.Debugf("IsNodeBad [%s]: recent pieces have taken too long to download avg[%d] last[%d]", peer.PeerID, avgCost, lastCost)
		return true
	}

	return false
}

// The bigger the better
func (eval *baseEvaluator) Evaluate(dst *peer.PeerNode, src *peer.PeerNode) float64 {
	profits := getProfits(dst, src)

	load := getHostLoad(dst.Host)

	dist := getDistance(dst, src)

	return profits * load * dist
}

func getAvgAndLastCost(list []int, splitPos int) (avgCost, lastCost int) {
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

// getProfits 0.0~unlimited larger and better
func getProfits(dst *peer.PeerNode, src *peer.PeerNode) float64 {
	diff := peer.GetDiffPieceNum(src, dst)
	depth := dst.GetDepth()

	return float64(int(diff+1)*src.GetWholeTreeNode()) / float64(depth*depth)
}

// getHostLoad 0.0~1.0 larger and better
func getHostLoad(host *host.NodeHost) float64 {
	return 1.0 - host.GetUploadLoadPercent()
}

// getDistance 0.0~1.0 larger and better
func getDistance(dst *peer.PeerNode, src *peer.PeerNode) float64 {
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
