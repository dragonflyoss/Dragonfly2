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
	"sort"
	"strings"

	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
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
)

const (
	// Maximum score.
	maxScore float64 = 1

	// Minimum score.
	minScore = 0
)

const (
	// Maximum number of elements.
	maxElementLen = 5
)

type evaluatorBase struct {
	BaseEvaluator
}

func NewEvaluatorBase() Evaluator {
	return &evaluatorBase{}
}

// EvaluateParents sort parents by evaluating multiple feature scores.
func (eb *evaluatorBase) EvaluateParents(parents []*resource.Peer, child *resource.Peer, totalPieceCount int32) []*resource.Peer {
	sort.Slice(
		parents,
		func(i, j int) bool {
			return evaluate(parents[i], child, totalPieceCount) > evaluate(parents[j], child, totalPieceCount)
		},
	)

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
