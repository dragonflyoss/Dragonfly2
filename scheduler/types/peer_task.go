package types

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
)

type PeerTask struct {
	Pid  string `json:"pid,omitempty"`       // peer id
	Task *Task  `json:"peer_host,omitempty"` // task info
	Host *Host  `json:"peer_host,omitempty"` // host info

	DownloadingPieceNumList []int32
	PieceStatusList map[int32]PieceStatus
	FirstPieceNum int32 //
	FinishedNum int32 // download finished piece number
}

type PeerHost struct {
	Pid  string `json:"pid,omitempty"`       // peer id
	Host *Host  `json:"peer_host,omitempty"` // host info
}

type PieceStatus struct {
	SrcPid     string    `protobuf:"bytes,2,opt,name=src_pid,json=srcPid,proto3" json:"src_pid,omitempty"`
	Success    bool      `protobuf:"varint,6,opt,name=success,proto3" json:"success,omitempty"`
	ErrorCode  base.Code `protobuf:"varint,7,opt,name=error_code,json=errorCode,proto3,enum=base.Code" json:"error_code,omitempty"` // for success is false
	Cost       uint32    `protobuf:"varint,8,opt,name=cost,proto3" json:"cost,omitempty"`
}