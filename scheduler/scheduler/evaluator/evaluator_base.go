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
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// Finished piece weight.
	finishedPieceWeight float64 = 0.3

	// Free load weight.
	freeLoadWeight = 0.2

	// Host type affinity weight.
	hostTypeAffinityWeight = 0.2

	// IDC affinity weight.
	idcAffinityWeight = 0.15

	// NetTopology affinity weight.
	netTopologyAffinityWeight = 0.1

	// Location affinity weight.
	locationAffinityWeight = 0.05
)

const (
	// Maximum score.
	maxScore float64 = 1

	// Minimum score.
	minScore = 0
)

const (
	// If the number of samples is greater than or equal to 30,
	// it is close to the normal distribution.
	normalDistributionLen = 30

	// When costs len is greater than or equal to 2,
	// the last cost can be compared and calculated.
	minAvailableCostLen = 2

	// Maximum number of elements.
	maxElementLen = 5
)

type evaluatorBase struct{}

func NewEvaluatorBase() Evaluator {
	return &evaluatorBase{}
}

// The larger the value after evaluation, the higher the priority.
func (eb *evaluatorBase) Evaluate(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
	// If the SecurityDomain of hosts exists but is not equal,
	// it cannot be scheduled as a parent.
	if parent.Host.SecurityDomain != "" &&
		child.Host.SecurityDomain != "" &&
		parent.Host.SecurityDomain != child.Host.SecurityDomain {
		return minScore
	}

	return finishedPieceWeight*calculatePieceScore(parent, child, totalPieceCount) +
		freeLoadWeight*calculateFreeLoadScore(parent.Host) +
		hostTypeAffinityWeight*calculateHostTypeAffinityScore(parent) +
		idcAffinityWeight*calculateIDCAffinityScore(parent.Host, child.Host) +
		netTopologyAffinityWeight*calculateMultiElementAffinityScore(parent.Host.NetTopology, child.Host.NetTopology) +
		locationAffinityWeight*calculateMultiElementAffinityScore(parent.Host.Location, child.Host.Location)
}

// calculatePieceScore 0.0~unlimited larger and better.
func calculatePieceScore(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node.
	if totalPieceCount > 0 {
		finishedPieceCount := parent.Pieces.Count()
		return float64(finishedPieceCount) / float64(totalPieceCount)
	}

	// Use the difference between the parent node and the child node to
	// download the piece to roughly represent the piece score.
	parentFinishedPieceCount := parent.Pieces.Count()
	childFinishedPieceCount := child.Pieces.Count()
	return float64(parentFinishedPieceCount) - float64(childFinishedPieceCount)
}

// calculateFreeLoadScore 0.0~1.0 larger and better.
func calculateFreeLoadScore(host *resource.Host) float64 {
	uploadLoadLimit := host.UploadLoadLimit.Load()
	freeUploadLoad := host.FreeUploadLoad()
	if uploadLoadLimit > 0 && freeUploadLoad > 0 {
		return float64(freeUploadLoad) / float64(uploadLoadLimit)
	}

	return minScore
}

// calculateHostTypeAffinityScore 0.0~1.0 larger and better.
func calculateHostTypeAffinityScore(peer *resource.Peer) float64 {
	// When the task is downloaded for the first time,
	// peer will be scheduled to seed peer first,
	// otherwise it will be scheduled to dfdaemon first.
	if peer.Host.Type != resource.HostTypeNormal {
		if peer.FSM.Is(resource.PeerStateReceivedNormal) ||
			peer.FSM.Is(resource.PeerStateRunning) {
			return maxScore
		}

		return minScore
	}

	return maxScore * 0.5
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func calculateIDCAffinityScore(dst, src *resource.Host) float64 {
	if dst.IDC != "" && src.IDC != "" && dst.IDC == src.IDC {
		return maxScore
	}

	return minScore
}

// calculateMultiElementAffinityScore 0.0~1.0 larger and better.
func calculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if dst == src {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, "|")
	srcElements := strings.Split(src, "|")
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 5.
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if dstElements[i] != srcElements[i] {
			break
		}
		score++
	}

	return float64(score) / float64(maxElementLen)
}

func (eb *evaluatorBase) IsBadNode(peer *resource.Peer) bool {
	if peer.FSM.Is(resource.PeerStateFailed) || peer.FSM.Is(resource.PeerStateLeave) || peer.FSM.Is(resource.PeerStatePending) ||
		peer.FSM.Is(resource.PeerStateReceivedTiny) || peer.FSM.Is(resource.PeerStateReceivedSmall) || peer.FSM.Is(resource.PeerStateReceivedNormal) {
		peer.Log.Debugf("peer is bad node because peer status is %s", peer.FSM.Current())
		return true
	}

	// Determine whether to bad node based on piece download costs.
	costs := stats.LoadRawData(peer.PieceCosts())
	len := len(costs)
	// Peer has not finished downloading enough piece.
	if len < minAvailableCostLen {
		logger.Debugf("peer %s has not finished downloading enough piece, it can't be bad node", peer.ID)
		return false
	}

	lastCost := costs[len-1]
	mean, _ := stats.Mean(costs[:len-1]) // nolint: errcheck

	// Download costs does not meet the normal distribution,
	// if the last cost is twenty times more than mean, it is bad node.
	if len < normalDistributionLen {
		isBadNode := big.NewFloat(lastCost).Cmp(big.NewFloat(mean*20)) > 0
		logger.Debugf("peer %s mean is %.2f and it is bad node: %t", peer.ID, mean, isBadNode)
		return isBadNode
	}

	// Download costs satisfies the normal distribution,
	// last cost falling outside of three-sigma effect need to be adjusted parent,
	// refer to https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule.
	stdev, _ := stats.StandardDeviation(costs[:len-1]) // nolint: errcheck
	isBadNode := big.NewFloat(lastCost).Cmp(big.NewFloat(mean+3*stdev)) > 0
	logger.Debugf("peer %s meet the normal distribution, costs mean is %.2f and standard deviation is %.2f, peer is bad node: %t",
		peer.ID, mean, stdev, isBadNode)
	return isBadNode
}
