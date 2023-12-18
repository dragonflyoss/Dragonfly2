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
	"sort"
	"strings"
	"time"

	"github.com/montanaflynn/stats"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// Finished piece weight.
	finishedPieceWeight float64 = 0.2

	// Parent's host upload success weight.
	parentHostUploadSuccessWeight = 0.2

	// Free upload weight.
	freeUploadWeight = 0.15

	// Host type weight.
	hostTypeWeight = 0.15

	// IDC affinity weight.
	idcAffinityWeight = 0.15

	// Location affinity weight.
	locationAffinityWeight = 0.15

	// Network topology weight.
	networkTopologyWeight = 0.05
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

type evaluatorBase struct {
	networktopology networktopology.NetworkTopology
}

type Option func(eb *evaluatorBase)

// WithNetworkTopology sets the networkTopology.
func WithNetworkTopology(networktopology networktopology.NetworkTopology) Option {
	return func(eb *evaluatorBase) {
		eb.networktopology = networktopology
	}
}

func NewEvaluatorBase(options ...Option) Evaluator {
	eb := &evaluatorBase{}

	for _, opt := range options {
		opt(eb)
	}
	return eb
}

// EvaluateParents sort parents by evaluating multiple feature scores.
func (eb *evaluatorBase) EvaluateParents(parents []*resource.Peer, child *resource.Peer, totalPieceCount int32) []*resource.Peer {
	if eb.networktopology == nil {
		sort.Slice(
			parents,
			func(i, j int) bool {
				return evaluate(parents[i], child, totalPieceCount) > evaluate(parents[j], child, totalPieceCount)
			},
		)
	} else {
		var parentIDs []string
		for _, parent := range parents {
			parentIDs = append(parentIDs, parent.ID)
		}
		scoces := eb.calculateNetworkTopologyScore(child.ID, parentIDs)

		sort.Slice(
			parents,
			func(i, j int) bool {
				return (evaluate(parents[i], child, totalPieceCount) + networkTopologyWeight*scoces[i]) > (evaluate(parents[j], child, totalPieceCount) + networkTopologyWeight*scoces[j])
			},
		)
	}

	return parents
}

// The larger the value, the higher the priority.
func evaluate(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
	parentLocation := parent.Host.Network.Location
	parentIDC := parent.Host.Network.IDC
	childLocation := child.Host.Network.Location
	childIDC := child.Host.Network.IDC

	return finishedPieceWeight*calculatePieceScore(parent, child, totalPieceCount) +
		parentHostUploadSuccessWeight*calculateParentHostUploadSuccessScore(parent) +
		freeUploadWeight*calculateFreeUploadScore(parent.Host) +
		hostTypeWeight*calculateHostTypeScore(parent) +
		idcAffinityWeight*calculateIDCAffinityScore(parentIDC, childIDC) +
		locationAffinityWeight*calculateMultiElementAffinityScore(parentLocation, childLocation)
}

// calculatePieceScore 0.0~unlimited larger and better.
func calculatePieceScore(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node.
	if totalPieceCount > 0 {
		finishedPieceCount := parent.FinishedPieces.Count()
		return float64(finishedPieceCount) / float64(totalPieceCount)
	}

	// Use the difference between the parent node and the child node to
	// download the piece to roughly represent the piece score.
	parentFinishedPieceCount := parent.FinishedPieces.Count()
	childFinishedPieceCount := child.FinishedPieces.Count()
	return float64(parentFinishedPieceCount) - float64(childFinishedPieceCount)
}

// calculateParentHostUploadSuccessScore 0.0~unlimited larger and better.
func calculateParentHostUploadSuccessScore(peer *resource.Peer) float64 {
	uploadCount := peer.Host.UploadCount.Load()
	uploadFailedCount := peer.Host.UploadFailedCount.Load()
	if uploadCount < uploadFailedCount {
		return minScore
	}

	// Host has not been scheduled, then it is scheduled first.
	if uploadCount == 0 && uploadFailedCount == 0 {
		return maxScore
	}

	return float64(uploadCount-uploadFailedCount) / float64(uploadCount)
}

// calculateFreeUploadScore 0.0~1.0 larger and better.
func calculateFreeUploadScore(host *resource.Host) float64 {
	ConcurrentUploadLimit := host.ConcurrentUploadLimit.Load()
	freeUploadCount := host.FreeUploadCount()
	if ConcurrentUploadLimit > 0 && freeUploadCount > 0 {
		return float64(freeUploadCount) / float64(ConcurrentUploadLimit)
	}

	return minScore
}

// calculateHostTypeScore 0.0~1.0 larger and better.
func calculateHostTypeScore(peer *resource.Peer) float64 {
	// When the task is downloaded for the first time,
	// peer will be scheduled to seed peer first,
	// otherwise it will be scheduled to dfdaemon first.
	if peer.Host.Type != types.HostTypeNormal {
		if peer.FSM.Is(resource.PeerStateReceivedNormal) ||
			peer.FSM.Is(resource.PeerStateRunning) {
			return maxScore
		}

		return minScore
	}

	return maxScore * 0.5
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func calculateIDCAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.EqualFold(dst, src) {
		return maxScore
	}

	return minScore
}

// calculateMultiElementAffinityScore 0.0~1.0 larger and better.
func calculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return minScore
	}

	if strings.EqualFold(dst, src) {
		return maxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 5.
	if elementLen > maxElementLen {
		elementLen = maxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if !strings.EqualFold(dstElements[i], srcElements[i]) {
			break
		}

		score++
	}

	return float64(score) / float64(maxElementLen)
}

// calculateNetworkTopologyScore 0.0~1.0 larger and better.
func (eb *evaluatorBase) calculateNetworkTopologyScore(src string, dst []string) []float64 {
	averageRTTs, err := eb.networktopology.AverageRTTs(src, dst)
	if err != nil {
		return []float64{}
	}

	var MaxRTT time.Duration
	for _, RTT := range averageRTTs {
		if MaxRTT < RTT {
			MaxRTT = RTT
		}
	}

	var scoces []float64
	for _, RTT := range averageRTTs {
		if RTT == 0 {
			scoces = append(scoces, minScore)
		} else {
			scoces = append(scoces, float64((MaxRTT-RTT))/float64(MaxRTT))
		}
	}

	return scoces
}

func (eb *evaluatorBase) IsBadNode(peer *resource.Peer) bool {
	if peer.FSM.Is(resource.PeerStateFailed) || peer.FSM.Is(resource.PeerStateLeave) || peer.FSM.Is(resource.PeerStatePending) ||
		peer.FSM.Is(resource.PeerStateReceivedTiny) || peer.FSM.Is(resource.PeerStateReceivedSmall) ||
		peer.FSM.Is(resource.PeerStateReceivedNormal) || peer.FSM.Is(resource.PeerStateReceivedEmpty) {
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
