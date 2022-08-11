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
	"strings"

	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const (
	// Finished piece weight.
	finishedPieceWeight float64 = 0.3

	// Free load weight.
	freeLoadWeight = 0.2

	// Host type weight.
	hostTypeWeight = 0.2

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
	return Evaluate(parent, child, totalPieceCount)
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

// calculateFreeLoadScore 0.0~1.0 larger and better.
func calculateFreeLoadScore(host *resource.Host) float64 {
	uploadLoadLimit := host.UploadLoadLimit.Load()
	freeUploadLoad := host.FreeUploadLoad()
	if uploadLoadLimit > 0 && freeUploadLoad > 0 {
		return float64(freeUploadLoad) / float64(uploadLoadLimit)
	}

	return minScore
}

// calculateHostTypeScore 0.0~1.0 larger and better.
func calculateHostTypeScore(peer *resource.Peer) float64 {
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
	dstElements := strings.Split(dst, types.AffinitySeparator)
	srcElements := strings.Split(src, types.AffinitySeparator)
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
	return NormalIsBadNode(peer)
}

func (eb *evaluatorBase) EvalType() string {
	return DefaultAlgorithm
}
