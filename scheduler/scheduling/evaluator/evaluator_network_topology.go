/*
 *     Copyright 2024 The Dragonfly Authors
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
	"sort"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// Finished piece weight.
	networkTopologyFinishedPieceWeight float64 = 0.2

	// Parent's host upload success weight.
	networkTopologyParentHostUploadSuccessWeight = 0.2

	// Free upload weight.
	networkTopologyFreeUploadWeight = 0.15

	// Network topology weight.
	networkTopologyNetworkTopologyWeight = 0.12

	// Host type weight.
	networkTopologyHostTypeWeight = 0.11

	// IDC affinity weight.
	networkTopologyIDCAffinityWeight = 0.11

	// Location affinity weight.
	networkTopologyLocationAffinityWeight = 0.11
)

const (
	// Maximum score.
	networkTopologyMaxScore float64 = 1

	// Minimum score.
	networkTopologyMinScore = 0
)

const (
	// Maximum number of elements.
	networkTopologyMaxElementLen = 5

	// defaultPingTimeout specifies a default timeout before ping exits.
	defaultPingTimeout = 1 * time.Second
)

type evaluatorNetworkTopology struct {
	BaseEvaluator
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

	return networkTopologyFinishedPieceWeight*networkTopologyCalculatePieceScore(parent, child, totalPieceCount) +
		networkTopologyParentHostUploadSuccessWeight*networkTopologyCalculateParentHostUploadSuccessScore(parent) +
		networkTopologyFreeUploadWeight*networkTopologyCalculateFreeUploadScore(parent.Host) +
		networkTopologyHostTypeWeight*networkTopologyCalculateHostTypeScore(parent) +
		networkTopologyIDCAffinityWeight*networkTopologyCalculateIDCAffinityScore(parentIDC, childIDC) +
		networkTopologyLocationAffinityWeight*networkTopologyCalculateMultiElementAffinityScore(parentLocation, childLocation) +
		networkTopologyNetworkTopologyWeight*en.networkTopologyCalculateNetworkTopologyScore(parent.ID, child.ID)
}

// networkTopologyCalculatePieceScore 0.0~unlimited larger and better.
func networkTopologyCalculatePieceScore(parent *resource.Peer, child *resource.Peer, totalPieceCount int32) float64 {
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

// networkTopologyCalculateParentHostUploadSuccessScore 0.0~unlimited larger and better.
func networkTopologyCalculateParentHostUploadSuccessScore(peer *resource.Peer) float64 {
	uploadCount := peer.Host.UploadCount.Load()
	uploadFailedCount := peer.Host.UploadFailedCount.Load()
	if uploadCount < uploadFailedCount {
		return networkTopologyMinScore
	}

	// Host has not been scheduled, then it is scheduled first.
	if uploadCount == 0 && uploadFailedCount == 0 {
		return networkTopologyMaxScore
	}

	return float64(uploadCount-uploadFailedCount) / float64(uploadCount)
}

// networkTopologyCalculateFreeUploadScore 0.0~1.0 larger and better.
func networkTopologyCalculateFreeUploadScore(host *resource.Host) float64 {
	ConcurrentUploadLimit := host.ConcurrentUploadLimit.Load()
	freeUploadCount := host.FreeUploadCount()
	if ConcurrentUploadLimit > 0 && freeUploadCount > 0 {
		return float64(freeUploadCount) / float64(ConcurrentUploadLimit)
	}

	return networkTopologyMinScore
}

// networkTopologyCalculateHostTypeScore 0.0~1.0 larger and better.
func networkTopologyCalculateHostTypeScore(peer *resource.Peer) float64 {
	// When the task is downloaded for the first time,
	// peer will be scheduled to seed peer first,
	// otherwise it will be scheduled to dfdaemon first.
	if peer.Host.Type != types.HostTypeNormal {
		if peer.FSM.Is(resource.PeerStateReceivedNormal) ||
			peer.FSM.Is(resource.PeerStateRunning) {
			return networkTopologyMaxScore
		}

		return networkTopologyMinScore
	}

	return networkTopologyMaxScore * 0.5
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func networkTopologyCalculateIDCAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return networkTopologyMinScore
	}

	if strings.EqualFold(dst, src) {
		return networkTopologyMaxScore
	}

	return networkTopologyMinScore
}

// networkTopologyCalculateMultiElementAffinityScore 0.0~1.0 larger and better.
func networkTopologyCalculateMultiElementAffinityScore(dst, src string) float64 {
	if dst == "" || src == "" {
		return networkTopologyMinScore
	}

	if strings.EqualFold(dst, src) {
		return networkTopologyMaxScore
	}

	// Calculate the number of multi-element matches divided by "|".
	var score, elementLen int
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
	elementLen = math.Min(len(dstElements), len(srcElements))

	// Maximum element length is 5.
	if elementLen > networkTopologyMaxElementLen {
		elementLen = networkTopologyMaxElementLen
	}

	for i := 0; i < elementLen; i++ {
		if !strings.EqualFold(dstElements[i], srcElements[i]) {
			break
		}

		score++
	}

	return float64(score) / float64(networkTopologyMaxElementLen)
}

// networkTopologyCalculateNetworkTopologyScore 0.0~1.0 larger and better.
func (en *evaluatorNetworkTopology) networkTopologyCalculateNetworkTopologyScore(dst, src string) float64 {
	averageRTT, err := en.networktopology.Probes(dst, src).AverageRTT()
	if err != nil {
		return networkTopologyMinScore
	}

	return float64(defaultPingTimeout-averageRTT) / float64(defaultPingTimeout)
}
