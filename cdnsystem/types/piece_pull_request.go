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


package types

type PiecePullRequest struct {

	TaskID string `json:"taskId,omitempty"`

	// the uploader peerID
	//
	DstPID string `json:"dstPID,omitempty"`

	// the range of specific piece in the task, example "0-45565".
	//
	PieceRange string `json:"pieceRange,omitempty"`

	// pieceResult It indicates whether the dfgetTask successfully download the piece.
	// It's only useful when `status` is `RUNNING`.
	//
	// Enum: [FAILED SUCCESS INVALID SEMISUC]
	PieceResult string `json:"pieceResult,omitempty"`
}
