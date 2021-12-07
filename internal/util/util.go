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

//TODO we should let following params make sense in future
const (
	// PieceSizeUpperBound 16M
	PieceSizeUpperBound = 1 << 24

	// PieceSizeLowerBound 4M
	PieceSizeLowerBound = 1 << 22

	// FileUpperBound 1G
	FileUpperBound = 1 << 30

	// FileLowerBound 256M
	FileLowerBound = 1 << 28
)

// ComputePieceSize computes the piece size with specified fileLength.
//
// If the fileLength<0, which means failed to get fileLength
// and then use the DefaultPieceSize.
// so the pieceSize will increase as the file size increases as shown followed
//  piece size (in MB) 16 ^            -----
//                        |          /
//                        |        /
//                        |      /
//                      4 |-----
//                        |---------------------->
//                        0     256   1024        file size (in MB)
func ComputePieceSize(length int64) uint32 {
	if length <= FileLowerBound {
		return PieceSizeLowerBound
	}
	if length >= FileUpperBound {
		return PieceSizeUpperBound
	}
	return uint32(length >> int64(6))
}
