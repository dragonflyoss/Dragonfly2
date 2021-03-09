package storage

import (
	"io"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

type persistentMetadata struct {
	StoreStrategy string
	TaskID        string                  `json:"taskID"`
	TaskMeta      map[string]string       `json:"taskMeta"`
	ContentLength int64                   `json:"contentLength"`
	TotalPieces   int32                   `json:"totalPieces"`
	PeerID        string                  `json:"peerID"`
	Pieces        map[int32]PieceMetaData `json:"pieces"`
	PieceMd5Sign  string
}

type PeerTaskMetaData struct {
	PeerID string `json:"peerID,omitempty"`
	TaskID string `json:"taskID,omitempty"`
}

type PieceMetaData struct {
	Num    int32            `json:"num,omitempty"`
	Md5    string           `json:"md5,omitempty"`
	Offset uint64           `json:"offset,omitempty"`
	Range  clientutil.Range `json:"range,omitempty"`
	Style  base.PieceStyle  `json:"style,omitempty"`
}

type CommonTaskRequest struct {
	PeerID      string `json:"peerID,omitempty"`
	TaskID      string `json:"taskID,omitempty"`
	Destination string
}

type RegisterTaskRequest struct {
	CommonTaskRequest
	ContentLength int64
	TotalPieces   int32
	GCCallback    func(CommonTaskRequest)
}

type WritePieceRequest struct {
	PeerTaskMetaData
	PieceMetaData
	UnknownLength bool
	Reader        io.Reader
}

type StoreRequest struct {
	CommonTaskRequest
	MetadataOnly bool
	TotalPieces  int32
}

type ReadPieceRequest struct {
	PeerTaskMetaData
	PieceMetaData
}

type UpdateTaskRequest struct {
	PeerTaskMetaData
	ContentLength int64
}
