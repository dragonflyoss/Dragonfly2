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

package evaluator

import (
	"math"
	"math/big"
	"strings"

	"github.com/montanaflynn/stats"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

const (
	// If the number of samples is greater than or equal to 10, it is close to the normal distribution
	normalDistributionLen = 10

	// When costs len is greater than or equal to 2, the last cost can be compared and calculated
	minAvailableCostLen = 2
)

const (
	// IDC distance weight
	idcDistanceWeight float64 = 0.5

	// NetTopology distance weight
	netTopologyDistanceWeight float64 = 0.3

	// Location distance weight
	locationDistanceWeight float64 = 0.2
)

const (
	// Maximum score
	maxScore float64 = 1

	// Minimum score
	minScore float64 = 0

	// Maximum number of elements
	maxElementLen int = 5
)

type evaluatorBase struct{}

func NewEvaluatorBase() Evaluator {
	return &evaluatorBase{}
}

// The larger the value after evaluation, the higher the priority
func (eb *evaluatorBase) Evaluate(parent *supervisor.Peer, child *supervisor.Peer) float64 {
	// If the SecurityDomain of hosts exists but is not equal,
	// it cannot be scheduled as a parent
	if parent.Host.SecurityDomain != "" &&
		child.Host.SecurityDomain != "" &&
		strings.Compare(parent.Host.SecurityDomain, child.Host.SecurityDomain) != 0 {
		return minScore
	}

	// Profits has the highest priority, FreeLoad Percent and Distance have the same priority
	return calculateProfits(parent, child) + calculateFreeLoadPercent(parent.Host) + calculateDistance(parent, child)
}

// calculateProfits 0.0~unlimited larger and better
func calculateProfits(dst *supervisor.Peer, src *supervisor.Peer) float64 {
	diff := dst.TotalPieceCount.Load() - src.TotalPieceCount.Load()
	depth := dst.GetTreeDepth()
	return float64(int(diff+1)*src.GetTreeNodeCount()) / math.Pow(float64(depth), 2)
}

// calculateFreeLoadPercent 0.0~1.0 larger and better
func calculateFreeLoadPercent(host *supervisor.Host) float64 {
	load := host.CurrentUploadLoad.Load()
	totalLoad := host.TotalUploadLoad
	return float64((totalLoad - load)) / float64(totalLoad)
}

// calculateDistance 0.0~1.0 larger and better
func calculateDistance(dst *supervisor.Peer, src *supervisor.Peer) float64 {
	// If the host is equal, it means that the same host downloads the same peer
	// and the host is preferentially scheduled
	if strings.Compare(dst.Host.UUID, src.Host.UUID) == 0 {
		return maxScore
	}

	// Distance includes three features, the specific values of the features are IDC, NetTopology and Location.
	// Based on three features and specific weights, the distance value between 0 and 1 can be calculated.
	return calculateIDCDistance(dst.Host.IDC, src.Host.IDC)*idcDistanceWeight +
		calculateMultiElementDistance(dst.Host.NetTopology, src.Host.NetTopology)*netTopologyDistanceWeight +
		calculateMultiElementDistance(dst.Host.Location, src.Host.Location)*locationDistanceWeight
}

// calculateDistance 0.0~1.0 larger and better
func calculateIDCDistance(dst, src string) float64 {
	if dst != "" && src != "" && strings.Compare(dst, src) == 0 {
		return maxScore
	}

	return minScore
}

// calculateMultiElementDistance 0.0~1.0 larger and better
func calculateMultiElementDistance(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.Compare(dst, src) == 0 {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|"
	var score, elementLen int
	dstElements := strings.Split(dst, "|")
	srcElements := strings.Split(src, "|")
	elementLen = mathutils.MaxInt(len(dstElements), len(srcElements))

	// Maximum element length is 5
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if strings.Compare(dstElements[i], srcElements[i]) != 0 {
			break
		}
		score++
	}

	return float64(score) / float64(maxElementLen)
}

func (eb *evaluatorBase) NeedAdjustParent(peer *supervisor.Peer) bool {
	// CDN is the root node
	if peer.Host.IsCDN {
		return false
	}

	parent, ok := peer.GetParent()
	// Peer has no parent and is not completed
	if !ok && !peer.IsDone() {
		logger.Infof("peer %s need adjust parent because it has not parent and status is %s", peer.ID, peer.GetStatus())
		return true
	}

	// Peer has parent but parent can't be scheduled.
	if ok && (parent.IsLeave() || eb.IsBadNode(parent)) {
		logger.Infof("peer %s need adjust parent because parent can't be scheduled", peer.ID)
		return true
	}

	// Determine whether to adjust parent based on piece download costs
	rawCosts := peer.GetPieceCosts()
	costs := stats.LoadRawData(rawCosts)
	len := len(costs)
	// Peer has not finished downloading enough piece
	if len < minAvailableCostLen {
		logger.Infof("peer %s has not finished downloading enough piece, it can't be adjusted parent", peer.ID)
		return false
	}

	lastCost := costs[len-1]
	mean, _ := stats.Mean(costs[:len-1]) // nolint: errcheck

	// Download costs does not meet the normal distribution,
	// if the last cost is five times more than mean, it need to be adjusted parent.
	if len < normalDistributionLen {
		isNeedAdjustParent := big.NewFloat(lastCost).Cmp(big.NewFloat(mean*5)) > 0
		logger.Infof("peer %s does not meet the normal distribution and mean is %.2f, peer need adjust parent: %t", peer.ID, mean, isNeedAdjustParent)
		return isNeedAdjustParent
	}

	// Download costs satisfies the normal distribution,
	// last cost falling outside of three-sigma effect need to be adjusted parent,
	// refer to https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule
	stdev, _ := stats.StandardDeviation(costs[:len-2]) // nolint: errcheck
	isNeedAdjustParent := big.NewFloat(lastCost).Cmp(big.NewFloat(mean+3*stdev)) > 0
	logger.Infof("peer %s meet the normal distribution, costs mean is %.2f and standard deviation is %.2f, peer need adjust parent: %t",
		peer.ID, mean, stdev, isNeedAdjustParent)
	return isNeedAdjustParent
}

func (eb *evaluatorBase) IsBadNode(peer *supervisor.Peer) bool {
	if peer.IsBad() {
		logger.Infof("peer %s is bad because it's status is %s", peer.ID, peer.GetStatus())
		return true
	}

	if peer.Host.IsCDN {
		logger.Infof("peer %s is cdn can't be bad node", peer.ID)
		return false
	}

	// Determine whether to bad node based on piece download costs
	rawCosts := peer.GetPieceCosts()
	costs := stats.LoadRawData(rawCosts)
	len := len(costs)
	// Peer has not finished downloading enough piece
	if len < minAvailableCostLen {
		logger.Infof("peer %s has not finished downloading enough piece, it can't be bad node", peer.ID)
		return false
	}

	lastCost := costs[len-1]
	mean, _ := stats.Mean(costs[:len-1]) // nolint: errcheck

	// Download costs does not meet the normal distribution,
	// if the last cost is forty times more than mean, it is bad node.
	isBadNode := big.NewFloat(lastCost).Cmp(big.NewFloat(mean*40)) > 0
	logger.Infof("peer %s mean is %.2f and it is bad node: %t", peer.ID, mean, isBadNode)
	return isBadNode
}
