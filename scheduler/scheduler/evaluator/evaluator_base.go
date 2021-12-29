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
	"math/big"
	"strings"

	"github.com/montanaflynn/stats"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"d7y.io/dragonfly/v2/scheduler/entity"
)

const (
	// Finished piece weight
	finishedPieceWeight float64 = 0.4

	// Free load weight
	freeLoadWeight = 0.3

	// IDC affinity weight
	idcAffinityWeight = 0.15

	// NetTopology affinity weight
	netTopologyAffinityWeight = 0.1

	// Location affinity weight
	locationAffinityWeight = 0.05
)

const (
	// Maximum score
	maxScore float64 = 1

	// Minimum score
	minScore = 0
)

const (
	// If the number of samples is greater than or equal to 10,
	// it is close to the normal distribution
	normalDistributionLen = 10

	// When costs len is greater than or equal to 2,
	// the last cost can be compared and calculated
	minAvailableCostLen = 2

	// Maximum number of elements
	maxElementLen = 5
)

type evaluatorBase struct{}

func NewEvaluatorBase() Evaluator {
	return &evaluatorBase{}
}

// The larger the value after evaluation, the higher the priority
func (eb *evaluatorBase) Evaluate(parent *entity.Peer, child *entity.Peer, taskPieceCount int32) float64 {
	// If the SecurityDomain of hosts exists but is not equal,
	// it cannot be scheduled as a parent
	if parent.Host.SecurityDomain != "" &&
		child.Host.SecurityDomain != "" &&
		strings.Compare(parent.Host.SecurityDomain, child.Host.SecurityDomain) != 0 {
		return minScore
	}

	return finishedPieceWeight*calculatePieceScore(parent, child, taskPieceCount) +
		freeLoadWeight*calculateFreeLoadScore(parent.Host) +
		idcAffinityWeight*calculateIDCAffinityScore(parent.Host, child.Host) +
		netTopologyAffinityWeight*calculateMultiElementAffinityScore(parent.Host.NetTopology, child.Host.NetTopology) +
		locationAffinityWeight*calculateMultiElementAffinityScore(parent.Host.Location, child.Host.Location)
}

// calculatePieceScore 0.0~unlimited larger and better
func calculatePieceScore(parent *entity.Peer, child *entity.Peer, taskPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node
	if taskPieceCount > 0 {
		finishedPieceCount := parent.Pieces.Count()
		return float64(finishedPieceCount) / float64(taskPieceCount)
	}

	// Use the difference between the parent node and the child node to
	// download the piece to roughly represent the piece score
	parentFinishedPieceCount := parent.Pieces.Count()
	childFinishedPieceCount := child.Pieces.Count()
	return float64(parentFinishedPieceCount - childFinishedPieceCount)
}

// calculateFreeLoadScore 0.0~1.0 larger and better
func calculateFreeLoadScore(host *entity.Host) float64 {
	load := host.LenPeers()
	totalLoad := host.UploadLoadLimit.Load()
	return float64(totalLoad-int32(load)) / float64(totalLoad)
}

// calculateIDCAffinityScore 0.0~1.0 larger and better
func calculateIDCAffinityScore(dst, src *entity.Host) float64 {
	if dst.IDC != "" && src.IDC != "" && strings.Compare(dst.IDC, src.IDC) == 0 {
		return maxScore
	}

	return minScore
}

// calculateMultiElementAffinityScore 0.0~1.0 larger and better
func calculateMultiElementAffinityScore(dst, src string) float64 {
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

func (eb *evaluatorBase) IsBadNode(peer *entity.Peer) bool {
	if peer.FSM.Is(entity.PeerStateFailed) || peer.FSM.Is(entity.PeerStateLeave) || peer.FSM.Is(entity.PeerStatePending) {
		peer.Log.Infof("peer is bad node because peer status is %s", peer.FSM.Current())
		return true
	}

	// Determine whether to bad node based on piece download costs
	rawCosts := peer.PieceCosts.Values()
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
