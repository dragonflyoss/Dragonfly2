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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

type baseEvaluator struct {
	cfg *config.SchedulerConfig
}

func NewEvaluator(cfg *config.SchedulerConfig) evaluator.Evaluator {
	eval := &baseEvaluator{cfg: cfg}
	logger.Debugf("create basic evaluator successfully")
	return eval
}

func (eval *baseEvaluator) NeedAdjustParent(peer *supervisor.Peer) bool {
	if peer.Host.IsCDN {
		return false
	}

	if peer.GetParent() == nil && !peer.IsDone() {
		logger.Debugf("peer %s need adjust parent because it has not parent and status is %s", peer.ID, peer.GetStatus())
		return true
	}
	// TODO Check whether the parent node is in the blacklist
	if peer.GetParent() != nil && eval.IsBadNode(peer.GetParent()) {
		logger.Debugf("peer %s need adjust parent because it current parent is bad", peer.ID)
		return true
	}

	if peer.GetParent() != nil && peer.GetParent().IsLeave() {
		logger.Debugf("peer %s need adjust parent because it current parent is status is leave", peer.ID)
		return true
	}

	costHistory := peer.GetCostHistory()
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := getAvgAndLastCost(costHistory, 4)
	// TODO adjust policy
	if (avgCost * 20) < lastCost {
		logger.Debugf("peer %s need adjust parent because it latest download cost is too time consuming", peer.ID)
		return true
	}
	return false
}

func (eval *baseEvaluator) IsBadNode(peer *supervisor.Peer) bool {
	if peer.IsBad() {
		logger.Debugf("peer %s is bad because it's status is %s", peer.ID, peer.GetStatus())
		return true
	}
	costHistory := peer.GetCostHistory()
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := getAvgAndLastCost(costHistory, 4)

	if avgCost*40 < lastCost && !peer.Host.IsCDN {
		logger.Debugf("peer %s is bad because recent pieces have taken too long to download avg[%d] last[%d]", peer.ID, avgCost, lastCost)
		return true
	}

	return false
}

// Evaluate The bigger, the better
func (eval *baseEvaluator) Evaluate(parent *supervisor.Peer, child *supervisor.Peer) float64 {
	profits := getProfits(parent, child)

	load := getHostLoad(parent.Host)

	dist := getDistance(parent, child)

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
func getProfits(dst *supervisor.Peer, src *supervisor.Peer) float64 {
	diff := supervisor.GetDiffPieceNum(dst, src)
	depth := dst.GetTreeDepth()

	return float64(int(diff+1)*src.GetTreeLen()) / float64(depth*depth)
}

// getHostLoad 0.0~1.0 larger and better
func getHostLoad(host *supervisor.Host) float64 {
	return 1.0 - host.GetUploadLoadPercent()
}

// getDistance 0.0~1.0 larger and better
func getDistance(dst *supervisor.Peer, src *supervisor.Peer) float64 {
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
