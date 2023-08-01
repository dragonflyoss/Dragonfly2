/*
 *     Copyright 2023 The Dragonfly Authors
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

package training

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/math"
	"d7y.io/dragonfly/v2/pkg/types"
)

const (
	// locationSeparator is location separator.
	locationSeparator = "|"

	// Maximum number of elements.
	maxElementLen = 3
)

const (
	// Maximum score.
	maxScore float64 = 1

	// Minimum score.
	minScore = 0
)

// calculatePieceScore 0.0~unlimited larger and better.
func calculatePieceScore(parentFinishedPieceCount, childFinishedPieceCount, totalPieceCount int32) float64 {
	// If the total piece is determined, normalize the number of
	// pieces downloaded by the parent node.
	if totalPieceCount > 0 {
		return float64(parentFinishedPieceCount) / float64(totalPieceCount)
	}

	// Use the difference between the parent node and the child node to
	// download the piece to roughly represent the piece score.
	return float64(parentFinishedPieceCount) - float64(childFinishedPieceCount)
}

// calculateParentHostUploadSuccessScore 0.0~unlimited larger and better.
func calculateParentHostUploadSuccessScore(uploadCount, uploadFailedCount int64) float64 {
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
func calculateFreeUploadScore(concurrentUploadLimit, concurrentUploadCount int32) float64 {
	freeUploadCount := concurrentUploadLimit - concurrentUploadCount
	if concurrentUploadLimit > 0 && freeUploadCount > 0 {
		return float64(freeUploadCount) / float64(concurrentUploadLimit)
	}

	return minScore
}

// calculateIDCAffinityScore 0.0~1.0 larger and better.
func calculateIDCAffinityScore(dst, src string) float64 {
	if dst != "" && src != "" && dst == src {
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

	// Maximum element length is 3.
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

// ipFeature convert an ip address to a feature vector.
func ipFeature(data string) ([]uint32, error) {
	ip := net.ParseIP(data)
	var features = make([]uint32, net.IPv6len)
	if l := len(ip); l != net.IPv4len && l != net.IPv6len {
		msg := fmt.Sprintf("invalid IP address: %s", ip)
		logger.Error(msg)
		return features, errors.New(msg)
	}

	if ip.To4() != nil {
		for i := 0; i < net.IPv4len; i++ {
			features[i] = uint32(ip[i])
		}
	}

	if ip.To16() != nil {
		for i := 0; i < net.IPv6len; i++ {
			features[i] = uint32(ip[i])
		}
	}

	return features, nil
}

// locationFeature converts location to a feature vector.
func locationFeature(location string) []uint32 {
	locs := strings.Split(location, locationSeparator)
	var features = make([]uint32, maxElementLen)
	for i, part := range locs {
		features[i] = hash(part)
	}

	return features
}

// idcFeature converts idc to a feature vector.
func idcFeature(idc string) uint32 {
	return hash(idc)
}

// hash convert string to uint64.
func hash(s string) uint32 {
	h := sha1.New()
	h.Write([]byte(s))

	return binary.LittleEndian.Uint32(h.Sum(nil))
}
