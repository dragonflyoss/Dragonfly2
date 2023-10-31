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

package storage

import (
	"io"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/source"
)

type persistentMetadata struct {
	StoreStrategy string                  `json:"storeStrategy"`
	TaskID        string                  `json:"taskID"`
	TaskMeta      map[string]string       `json:"taskMeta"`
	ContentLength int64                   `json:"contentLength"`
	TotalPieces   int32                   `json:"totalPieces"`
	PeerID        string                  `json:"peerID"`
	Pieces        map[int32]PieceMetadata `json:"pieces"`
	PieceMd5Sign  string                  `json:"pieceMd5Sign"`
	DataFilePath  string                  `json:"dataFilePath"`
	Done          bool                    `json:"done"`
	Header        *source.Header          `json:"header"`
}

type PeerTaskMetadata struct {
	PeerID string `json:"peerID,omitempty"`
	TaskID string `json:"taskID,omitempty"`
}

type PieceMetadata struct {
	Num    int32               `json:"num,omitempty"`
	Md5    string              `json:"md5,omitempty"`
	Offset uint64              `json:"offset,omitempty"`
	Range  http.Range          `json:"range,omitempty"`
	Style  commonv1.PieceStyle `json:"style,omitempty"`
	// time(nanosecond) consumed
	Cost uint64 `json:"cost,omitempty"`
}

type CommonTaskRequest struct {
	PeerID      string `json:"peerID,omitempty"`
	TaskID      string `json:"taskID,omitempty"`
	Destination string
}

type RegisterTaskRequest struct {
	PeerTaskMetadata
	DesiredLocation string
	ContentLength   int64
	TotalPieces     int32
	PieceMd5Sign    string
}

type WritePieceRequest struct {
	PeerTaskMetadata
	PieceMetadata
	UnknownLength bool
	Reader        io.Reader
	// NeedGenMetadata is used after the last piece in back source case
	NeedGenMetadata func(n int64) (total int32, contentLength int64, gen bool)
}

type StoreRequest struct {
	CommonTaskRequest
	MetadataOnly bool
	// StoreDataOnly stands save file only without save metadata, used in reuse cases
	StoreDataOnly bool
	TotalPieces   int32
	// OriginalOffset stands keep original offset in the target file, if the target file is not original file, return error
	OriginalOffset bool
}

type ReadPieceRequest struct {
	PeerTaskMetadata
	PieceMetadata
}

type ReadAllPiecesRequest struct {
	PeerTaskMetadata
	Range *http.Range
}

type RegisterSubTaskRequest struct {
	Parent  PeerTaskMetadata
	SubTask PeerTaskMetadata
	Range   *http.Range
}

type UpdateTaskRequest struct {
	PeerTaskMetadata
	ContentLength int64
	TotalPieces   int32
	PieceMd5Sign  string
	Header        *source.Header
}

type ReusePeerTask struct {
	PeerTaskMetadata
	ContentLength int64
	TotalPieces   int32
	PieceMd5Sign  string
	Header        *source.Header
	Storage       TaskStorageDriver
}
