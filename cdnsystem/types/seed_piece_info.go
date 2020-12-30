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

// SeedPiece
type SeedPiece struct {
	Type             ItemType    // 1: piece 0: task
	PieceStyle       PieceFormat // 0: PlainUnspecified
	PieceNum         int32
	PieceMd5         string
	PieceRange       string
	PieceOffset      uint64
	PieceLen         int32
	Last             bool
	ContentLength    int64
	BackSourceLength int64
}

type ItemType int8

const (
	PieceType ItemType = 1
	TaskType  ItemType = 2
)

type PieceFormat int8

const (
	PlainUnspecified PieceFormat = 1
)
