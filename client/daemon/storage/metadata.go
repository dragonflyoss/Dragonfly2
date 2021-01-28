package storage

import (
	"io"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type persistentMetadata struct {
	StoreStrategy string
	TaskID        string                  `json:"taskID"`
	TaskMeta      map[string]string       `json:"taskMeta"`
	ContentLength int64                   `json:"contentLength"`
	PeerID        string                  `json:"peerID"`
	Pieces        map[int32]PieceMetaData `json:"pieces"`
	PieceMd5Sign  string
}

type PeerTaskMetaData struct {
	PeerID string `json:"peerID,omitempty"`
	TaskID string `json:"taskID,omitempty"`
}

type PieceMetaData struct {
	Num    int32           `json:"num,omitempty"`
	Md5    string          `json:"md5,omitempty"`
	Offset uint64          `json:"offset,omitempty"`
	Range  util.Range      `json:"range,omitempty"`
	Style  base.PieceStyle `json:"style,omitempty"`
}

type CommonTaskRequest struct {
	PeerID      string `json:"peerID,omitempty"`
	TaskID      string `json:"taskID,omitempty"`
	Destination string
}

type RegisterTaskRequest struct {
	CommonTaskRequest
	ContentLength int64
	GCCallback    func(CommonTaskRequest)
}

type WritePieceRequest struct {
	PeerTaskMetaData
	PieceMetaData
	Reader io.Reader
}

type StoreRequest = CommonTaskRequest

type ReadPieceRequest struct {
	PeerTaskMetaData
	PieceMetaData
}
