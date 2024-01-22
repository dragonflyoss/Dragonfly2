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
	networkTopologyBasedAlgorithmFinishedPieceWeight float64 = 0.2

	// Parent's host upload success weight.
	networkTopologyBasedAlgorithmParentHostUploadSuccessWeight = 0.2

	// Network topology weight.
	networkTopologyBasedAlgorithmNetworkTopologyWeight = 0.2

	// Free upload weight.
	networkTopologyBasedAlgorithmFreeUploadWeight = 0.1

	// Host type weight.
	networkTopologyBasedAlgorithmHostTypeWeight = 0.1

	// IDC affinity weight.
	networkTopologyBasedAlgorithmIDCAffinityWeight = 0.1

	// Location affinity weight.
	networkTopologyBasedAlgorithmLocationAffinityWeight = 0.1
)

const (
	// Maximum score.
	networkTopologyBasedAlgorithmMaxScore float64 = 1

	// Minimum score.
	networkTopologyBasedAlgorithmMinScore = 0
)

const (
	// If the number of samples is greater than or equal to 30,
	// it is close to the normal distribution.
	networkTopologyBasedAlgorithmNormalDistributionLen = 30

	// When costs len is greater than or equal to 2,
	// the last cost can be compared and calculated.
	networkTopologyBasedAlgorithmMinAvailableCostLen = 2

	// Maximum number of elements.
	networkTopologyBasedAlgorithmMaxElementLen = 5

	// defaultPingTimeout specifies a default timeout before ping exits.
	defaultPingTimeout = 1 * time.Second
)

type evaluatorNetworkTopology struct {
	networktopology networktopology.NetworkTopology
}

type Option func(en *evaluatorNetworkTopology)

// WithNetworkTopology sets the networkTopology.
func WithNetworkTopology(networktopology networktopology.NetworkTopology) Option {
	return func(en *evaluatorNetworkTopology) {
		en.networktopology = networktopology
	}
}

func NewEvaluatorNetworkTopology(options ...Option) Evaluator {
	en := &evaluatorNetworkTopology{}

	for _, opt := range options {
		opt(en)
	}
	return en
}

// EvaluateParents sort parents by evaluating multiple feature scores.
func (en *evaluatorNetworkTopology) EvaluateParents(parents []*resource.Peer, child *resource.Peer, totalPieceCount int32) []*resource.Peer {
	sort.Slice(
		parents,
		func(i, j int) bool {
			return en.evaluate(parents[i], child, totalPieceCount) > en.evaluate(parents[j], child, totalPieceCount)
		},
	)

	return parents
}

// The larger the value, the higher the priority.
func (en *evaluatorNetworkTopology) evaluate(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
	parentLocation := parent.Host.Network.Location
	parentIDC := parent.Host.Network.IDC
	childLocation := child.Host.Network.Location
	childIDC := child.Host.Network.IDC

	return networkTopologyBasedAlgorithmFinishedPieceWeight*networkTopologyBasedAlgorithmCalculatePieceScore(parent, child, totalPieceCount) +
		networkTopologyBasedAlgorithmParentHostUploadSuccessWeight*networkTopologyBasedAlgorithmCalculateParentHostUploadSuccessScore(parent) +
		networkTopologyBasedAlgorithmFreeUploadWeight*networkTopologyBasedAlgorithmCalculateFreeUploadScore(parent.Host) +
		networkTopologyBasedAlgorithmHostTypeWeight*networkTopologyBasedAlgorithmCalculateHostTypeScore(parent) +
		networkTopologyBasedAlgorithmIDCAffinityWeight*networkTopologyBasedAlgorithmCalculateIDCAffinityScore(parentIDC, childIDC) +
		networkTopologyBasedAlgorithmLocationAffinityWeight*networkTopologyBasedAlgorithmCalculateMultiElementAffinityScore(parentLocation, childLocation) +
		networkTopologyBasedAlgorithmNetworkTopologyWeight*en.networkTopologyBasedAlgorithmCalculateNetworkTopologyScore(parent.ID, child.ID)
}

// networkTopologyBasedAlgorithmCalculatePieceScore 0.0~unlimited larger and better.
func networkTopologyBasedAlgorithmCalculatePieceScore(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
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

// networkTopologyBasedAlgorithmCalculateParentHostUploadSuccessScore 0.0~unlimited larger and better.
func networkTopologyBasedAlgorithmCalculateParentHostUploadSuccessScore(peer *resource.Peer) float64 {
	uploadCount := peer.Host.UploadCount.Load()
	uploadFailedCount := peer.Host.UploadFailedCount.Load()
	if uploadCount < uploadFailedCount {
		return networkTopologyBasedAlgorithmMinScore
	}

	// Host has not been scheduled, then it is scheduled first.
	if uploadCount == 0 && uploadFailedCount == 0 {
		return networkTopologyBasedAlgorithmMaxScore
	}

	return float64(uploadCount-uploadFailedCount) / float64(uploadCount)
}

// networkTopologyBasedAlgorithmCalculateFreeUploadScore 0.0~1.0 larger and better.
func networkTopologyBasedAlgorithmCalculateFreeUploadScore(host *resource.Host) float64 {
	ConcurrentUploadLimit := host.ConcurrentUploadLimit.Load()
	freeUploadCount := host.FreeUploadCount()
	if ConcurrentUploadLimit > 0 && freeUploadCount > 0 {
		return float64(freeUploadCount) / float64(ConcurrentUploadLimit)
	}

	return networkTopologyBasedAlgorithmMinScore
}

// networkTopologyBasedAlgorithmCalculateHostTypeScore 0.0~1.0 larger and better.
func networkTopologyBasedAlgorithmCalculateHostTypeScore(peer *resource.Peer) float64 {
	// When the task is downloaded for the first time,
	// peer will be scheduled to seed peer first,
	// otherwise it will be scheduled to dfdaemon first.
	if peer.Host.Type != types.HostTypeNormal {
		if peer.FSM.Is(resource.PeerStateReceivedNormal) ||
			peer.FSM.Is(resource.PeerStateRunning) {
			return networkTopologyBasedAlgorithmMaxScore
		}

		return networkTopologyBasedAlgorithmMinScore
	}

	return networkTopologyBasedAlgorithmMaxScore * 0.5
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func networkTopologyBasedAlgorithmCalculateIDCAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return networkTopologyBasedAlgorithmMinScore
	}

	if strings.EqualFold(dst, src) {
		return networkTopologyBasedAlgorithmMaxScore
	}

	return networkTopologyBasedAlgorithmMinScore
}

// networkTopologyBasedAlgorithmCalculateMultiElementAffinityScore 0.0~1.0 larger and better.
func networkTopologyBasedAlgorithmCalculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return networkTopologyBasedAlgorithmMinScore
	}

	if strings.EqualFold(dst, src) {
		return networkTopologyBasedAlgorithmMaxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 5.
	if elementLen > networkTopologyBasedAlgorithmMaxElementLen {
		elementLen = networkTopologyBasedAlgorithmMaxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if !strings.EqualFold(dstElements[i], srcElements[i]) {
			break
		}

		score++
	}

	return float64(score) / float64(networkTopologyBasedAlgorithmMaxElementLen)
}

// networkTopologyBasedAlgorithmCalculateNetworkTopologyScore 0.0~1.0 larger and better.
func (en *evaluatorNetworkTopology) networkTopologyBasedAlgorithmCalculateNetworkTopologyScore(dst, src string) float64 {
	averageRTT, err := en.networktopology.Probes(dst, src).AverageRTT()
	if err != nil {
		return networkTopologyBasedAlgorithmMinScore
	}

	return float64(defaultPingTimeout-averageRTT) / float64(defaultPingTimeout)
}

func (en *evaluatorNetworkTopology) IsBadNode(peer *resource.Peer) bool {
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
	if len < networkTopologyBasedAlgorithmMinAvailableCostLen {
		logger.Debugf("peer %s has not finished downloading enough piece, it can't be bad node", peer.ID)
		return false
	}

	lastCost := costs[len-1]
	mean, _ := stats.Mean(costs[:len-1]) // nolint: errcheck

	// Download costs does not meet the normal distribution,
	// if the last cost is twenty times more than mean, it is bad node.
	if len < networkTopologyBasedAlgorithmNormalDistributionLen {
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
