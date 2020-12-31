package storage

import (
	"io"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type persistentPeerTaskMetadata struct {
	TaskID   string                  `json:"taskID"`
	TaskMeta map[string]string       `json:"taskMeta"`
	PeerID   string                  `json:"peerID"`
	Pieces   map[int32]PieceMetaData `json:"pieces"`
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

type RegisterTaskRequest = CommonTaskRequest

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
