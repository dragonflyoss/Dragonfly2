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

package util

import "math"

const (
	// DefaultPieceSize 4M
	DefaultPieceSize = 4 * 1024 * 1024

	// DefaultPieceSizeLimit 15M
	DefaultPieceSizeLimit = 15 * 1024 * 1024
)

// ComputePieceSize computes the piece size with specified fileLength.
//
// If the fileLength<0, which means failed to get fileLength
// and then use the DefaultPieceSize.
func ComputePieceSize(length int64) uint32 {
	if length <= 200*1024*1024 {
		return DefaultPieceSize
	}

	gapCount := length / int64(100*1024*1024)
	mpSize := (gapCount-2)*1024*1024 + DefaultPieceSize
	if mpSize > DefaultPieceSizeLimit {
		return DefaultPieceSizeLimit
	}
	return uint32(mpSize)
}

// ComputePieceCount returns piece count with given length and pieceSize
func ComputePieceCount(length int64, pieceSize uint32) int32 {
	return int32(math.Ceil(float64(length) / float64(pieceSize)))
}
