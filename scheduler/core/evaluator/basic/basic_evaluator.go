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
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/evaluator"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type baseEvaluator struct {
	cfg *config.SchedulerConfig
}

func NewEvaluator(cfg *config.SchedulerConfig) evaluator.Evaluator {
	eval := &baseEvaluator{cfg: cfg}
	logger.Debugf("create basic evaluator successfully")
	return eval
}

func (eval *baseEvaluator) NeedAdjustParent(peer *types.Peer) bool {
	if peer.Host.CDN {
		return false
	}

	if peer.GetParent() == nil && !peer.IsDone() {
		logger.Debugf("peer %s need adjust parent because it has not parent and status is %s", peer.PeerID, peer.GetStatus())
		return true
	}

	if peer.GetParent() != nil && eval.IsBadNode(peer.GetParent()) {
		logger.Debugf("peer %s need adjust parent because it current parent is bad", peer.PeerID)
		return true
	}

	if peer.GetParent() != nil && peer.GetParent().IsLeave() {
		logger.Debugf("peer %s need adjust parent because it current parent is status is leave", peer.PeerID)
		return true
	}

	costHistory := peer.GetCostHistory()
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := getAvgAndLastCost(costHistory, 4)
	if avgCost*40 < lastCost {
		logger.Debugf("peer %s is bad because recent pieces have taken too long to download", peer.PeerID)
	}
	// TODO adjust policy
	result := (avgCost * 20) < lastCost
	if result == true {
		logger.Debugf("peer %s need adjust parent because it latest download cost is too time consuming", peer.PeerID)
	}
	return result
}

func (eval *baseEvaluator) IsBadNode(peer *types.Peer) bool {
	if peer.Host.CDN {
		return false
	}

	if peer.IsBad() {
		logger.Debugf("peer %s is bad because status is %s", peer.PeerID, peer.GetStatus())
		return true
	}

	if peer.IsBlocking() {
		return false
	}

	parent := peer.GetParent()

	if parent == nil {
		return false
	}
	if time.Now().After(peer.GetLastAccessTime().Add(5 * time.Second)) {
		logger.Debugf("peer %s is bad because have elapsed %s > 5s since the last access", peer.PeerID, time.Now().Sub(peer.GetLastAccessTime()))
		return true
	}

	costHistory := parent.GetCostHistory()
	if len(costHistory) < 4 {
		return false
	}

	avgCost, lastCost := getAvgAndLastCost(costHistory, 4)

	if avgCost*40 < lastCost {
		logger.Debugf("peer %s is bad because recent pieces have taken too long to download avg[%d] last[%d]", peer.PeerID, avgCost, lastCost)
		return true
	}

	return false
}

// The bigger the better
func (eval *baseEvaluator) Evaluate(dst *types.Peer, src *types.Peer) float64 {
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
func getProfits(dst *types.Peer, src *types.Peer) float64 {
	diff := types.GetDiffPieceNum(src, dst)
	depth := dst.GetDepth()

	return float64(int(diff+1)*src.GetWholeTreeNode()) / float64(depth*depth)
}

// getHostLoad 0.0~1.0 larger and better
func getHostLoad(host *types.PeerHost) float64 {
	return 1.0 - host.GetUploadLoadPercent()
}

// getDistance 0.0~1.0 larger and better
func getDistance(dst *types.Peer, src *types.Peer) float64 {
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
