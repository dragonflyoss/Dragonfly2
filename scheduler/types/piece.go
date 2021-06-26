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

import (
	"d7y.io/dragonfly/v2/internal/rpc/base"
)

type WaitingType int

type Piece struct {
	PieceNum    int32      `protobuf:"varint,1,opt,name=piece_num,json=pieceNum,proto3" json:"piece_num,omitempty"`
	RangeStart  uint64     `protobuf:"varint,2,opt,name=range_start,json=rangeStart,proto3" json:"range_start,omitempty"`
	RangeSize   int32      `protobuf:"varint,3,opt,name=range_size,json=rangeSize,proto3" json:"range_size,omitempty"`
	PieceMd5    string     `protobuf:"bytes,4,opt,name=piece_md5,json=pieceMd5,proto3" json:"piece_md5,omitempty"`
	PieceOffset uint64     `protobuf:"varint,5,opt,name=piece_offset,json=pieceOffset,proto3" json:"piece_offset,omitempty"`
	PieceStyle  PieceStyle `protobuf:"varint,6,opt,name=piece_style,json=pieceStyle,proto3,enum=base.PieceStyle" json:"piece_style,omitempty"`
}

type PieceStyle int32

func newEmptyPiece(pieceNum int32) *Piece {
	return &Piece{
		PieceInfo: base.PieceInfo{PieceNum: pieceNum},
	}
}
