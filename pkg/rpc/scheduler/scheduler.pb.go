//
//     Copyright 2020 The Dragonfly Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.14.0
// source: pkg/rpc/scheduler/scheduler.proto

package scheduler

import (
	base "github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type PeerTaskRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Url string `protobuf:"bytes,1,opt,name=url,proto3" json:"url,omitempty"`
	// filter is a regex string,
	// used for task id generator, assimilating different urls
	Filter string `protobuf:"bytes,2,opt,name=filter,proto3" json:"filter,omitempty"`
	// biz_id is used for task id generator to distinguish same urls
	BizId    string         `protobuf:"bytes,3,opt,name=biz_id,json=bizId,proto3" json:"biz_id,omitempty"`
	UrlMata  *base.UrlMeta  `protobuf:"bytes,4,opt,name=url_mata,json=urlMata,proto3" json:"url_mata,omitempty"`
	PeerId   string         `protobuf:"bytes,5,opt,name=peer_id,json=peerId,proto3" json:"peer_id,omitempty"`
	PeerHost *PeerHost      `protobuf:"bytes,6,opt,name=peer_host,json=peerHost,proto3" json:"peer_host,omitempty"`
	HostLoad *base.HostLoad `protobuf:"bytes,7,opt,name=host_load,json=hostLoad,proto3" json:"host_load,omitempty"`
}

func (x *PeerTaskRequest) Reset() {
	*x = PeerTaskRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerTaskRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerTaskRequest) ProtoMessage() {}

func (x *PeerTaskRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerTaskRequest.ProtoReflect.Descriptor instead.
func (*PeerTaskRequest) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{0}
}

func (x *PeerTaskRequest) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

func (x *PeerTaskRequest) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

func (x *PeerTaskRequest) GetBizId() string {
	if x != nil {
		return x.BizId
	}
	return ""
}

func (x *PeerTaskRequest) GetUrlMata() *base.UrlMeta {
	if x != nil {
		return x.UrlMata
	}
	return nil
}

func (x *PeerTaskRequest) GetPeerId() string {
	if x != nil {
		return x.PeerId
	}
	return ""
}

func (x *PeerTaskRequest) GetPeerHost() *PeerHost {
	if x != nil {
		return x.PeerHost
	}
	return nil
}

func (x *PeerTaskRequest) GetHostLoad() *base.HostLoad {
	if x != nil {
		return x.HostLoad
	}
	return nil
}

type PeerHost struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// different uuid represents different peer host,
	// avoiding conflict, need check other information(ip、port...)
	Uuid string `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	Ip   string `protobuf:"bytes,2,opt,name=ip,proto3" json:"ip,omitempty"`
	// rpc service port for peer
	Port     int32  `protobuf:"varint,3,opt,name=port,proto3" json:"port,omitempty"`
	HostName string `protobuf:"bytes,4,opt,name=host_name,json=hostName,proto3" json:"host_name,omitempty"`
	// security isolation domain for network
	SecurityDomain string `protobuf:"bytes,5,opt,name=security_domain,json=securityDomain,proto3" json:"security_domain,omitempty"`
	// area|country|province|city|...
	Location string `protobuf:"bytes,6,opt,name=location,proto3" json:"location,omitempty"`
	Idc      string `protobuf:"bytes,7,opt,name=idc,proto3" json:"idc,omitempty"`
	// network device construct, xx|yy|zz
	Switch string `protobuf:"bytes,8,opt,name=switch,proto3" json:"switch,omitempty"`
}

func (x *PeerHost) Reset() {
	*x = PeerHost{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerHost) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerHost) ProtoMessage() {}

func (x *PeerHost) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerHost.ProtoReflect.Descriptor instead.
func (*PeerHost) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{1}
}

func (x *PeerHost) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

func (x *PeerHost) GetIp() string {
	if x != nil {
		return x.Ip
	}
	return ""
}

func (x *PeerHost) GetPort() int32 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *PeerHost) GetHostName() string {
	if x != nil {
		return x.HostName
	}
	return ""
}

func (x *PeerHost) GetSecurityDomain() string {
	if x != nil {
		return x.SecurityDomain
	}
	return ""
}

func (x *PeerHost) GetLocation() string {
	if x != nil {
		return x.Location
	}
	return ""
}

func (x *PeerHost) GetIdc() string {
	if x != nil {
		return x.Idc
	}
	return ""
}

func (x *PeerHost) GetSwitch() string {
	if x != nil {
		return x.Switch
	}
	return ""
}

type PeerPacket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	State  *base.ResponseState `protobuf:"bytes,1,opt,name=state,proto3" json:"state,omitempty"`
	TaskId string              `protobuf:"bytes,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	// source peer id
	SrcPid string `protobuf:"bytes,3,opt,name=src_pid,json=srcPid,proto3" json:"src_pid,omitempty"`
	// concurrent downloading count from main peer
	ParallelCount int32       `protobuf:"varint,4,opt,name=parallel_count,json=parallelCount,proto3" json:"parallel_count,omitempty"`
	MainPeer      *PeerHost   `protobuf:"bytes,5,opt,name=main_peer,json=mainPeer,proto3" json:"main_peer,omitempty"`
	StealPeers    []*PeerHost `protobuf:"bytes,6,rep,name=steal_peers,json=stealPeers,proto3" json:"steal_peers,omitempty"`
	// whether or not peer task is finish
	Done          bool  `protobuf:"varint,7,opt,name=done,proto3" json:"done,omitempty"`
	ContentLength int64 `protobuf:"varint,8,opt,name=content_length,json=contentLength,proto3" json:"content_length,omitempty"`
}

func (x *PeerPacket) Reset() {
	*x = PeerPacket{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerPacket) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerPacket) ProtoMessage() {}

func (x *PeerPacket) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerPacket.ProtoReflect.Descriptor instead.
func (*PeerPacket) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{2}
}

func (x *PeerPacket) GetState() *base.ResponseState {
	if x != nil {
		return x.State
	}
	return nil
}

func (x *PeerPacket) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PeerPacket) GetSrcPid() string {
	if x != nil {
		return x.SrcPid
	}
	return ""
}

func (x *PeerPacket) GetParallelCount() int32 {
	if x != nil {
		return x.ParallelCount
	}
	return 0
}

func (x *PeerPacket) GetMainPeer() *PeerHost {
	if x != nil {
		return x.MainPeer
	}
	return nil
}

func (x *PeerPacket) GetStealPeers() []*PeerHost {
	if x != nil {
		return x.StealPeers
	}
	return nil
}

func (x *PeerPacket) GetDone() bool {
	if x != nil {
		return x.Done
	}
	return false
}

func (x *PeerPacket) GetContentLength() int64 {
	if x != nil {
		return x.ContentLength
	}
	return 0
}

type PieceResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId     string         `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	SrcPid     string         `protobuf:"bytes,2,opt,name=src_pid,json=srcPid,proto3" json:"src_pid,omitempty"`
	DstPid     string         `protobuf:"bytes,3,opt,name=dst_pid,json=dstPid,proto3" json:"dst_pid,omitempty"`
	PieceNum   int32          `protobuf:"varint,4,opt,name=piece_num,json=pieceNum,proto3" json:"piece_num,omitempty"`
	PieceRange string         `protobuf:"bytes,5,opt,name=piece_range,json=pieceRange,proto3" json:"piece_range,omitempty"`
	BeginTime  uint64         `protobuf:"varint,6,opt,name=begin_time,json=beginTime,proto3" json:"begin_time,omitempty"`
	EndTime    uint64         `protobuf:"varint,7,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	Success    bool           `protobuf:"varint,8,opt,name=success,proto3" json:"success,omitempty"`
	Code       base.Code      `protobuf:"varint,9,opt,name=code,proto3,enum=base.Code" json:"code,omitempty"`
	HostLoad   *base.HostLoad `protobuf:"bytes,10,opt,name=host_load,json=hostLoad,proto3" json:"host_load,omitempty"`
}

func (x *PieceResult) Reset() {
	*x = PieceResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PieceResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PieceResult) ProtoMessage() {}

func (x *PieceResult) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PieceResult.ProtoReflect.Descriptor instead.
func (*PieceResult) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{3}
}

func (x *PieceResult) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PieceResult) GetSrcPid() string {
	if x != nil {
		return x.SrcPid
	}
	return ""
}

func (x *PieceResult) GetDstPid() string {
	if x != nil {
		return x.DstPid
	}
	return ""
}

func (x *PieceResult) GetPieceNum() int32 {
	if x != nil {
		return x.PieceNum
	}
	return 0
}

func (x *PieceResult) GetPieceRange() string {
	if x != nil {
		return x.PieceRange
	}
	return ""
}

func (x *PieceResult) GetBeginTime() uint64 {
	if x != nil {
		return x.BeginTime
	}
	return 0
}

func (x *PieceResult) GetEndTime() uint64 {
	if x != nil {
		return x.EndTime
	}
	return 0
}

func (x *PieceResult) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *PieceResult) GetCode() base.Code {
	if x != nil {
		return x.Code
	}
	return base.Code_X_UNSPECIFIED
}

func (x *PieceResult) GetHostLoad() *base.HostLoad {
	if x != nil {
		return x.HostLoad
	}
	return nil
}

type PeerResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId         string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	PeerId         string `protobuf:"bytes,2,opt,name=peer_id,json=peerId,proto3" json:"peer_id,omitempty"`
	SrcIp          string `protobuf:"bytes,3,opt,name=src_ip,json=srcIp,proto3" json:"src_ip,omitempty"`
	SecurityDomain string `protobuf:"bytes,4,opt,name=security_domain,json=securityDomain,proto3" json:"security_domain,omitempty"`
	Idc            string `protobuf:"bytes,5,opt,name=idc,proto3" json:"idc,omitempty"`
	Url            string `protobuf:"bytes,6,opt,name=url,proto3" json:"url,omitempty"`
	// total content length(byte)
	ContentLength int64 `protobuf:"varint,7,opt,name=content_length,json=contentLength,proto3" json:"content_length,omitempty"`
	// network traffic usage(byte)
	Traffic uint64 `protobuf:"varint,8,opt,name=traffic,proto3" json:"traffic,omitempty"`
	// millisecond unit
	Cost    uint32    `protobuf:"varint,9,opt,name=cost,proto3" json:"cost,omitempty"`
	Success bool      `protobuf:"varint,10,opt,name=success,proto3" json:"success,omitempty"`
	Code    base.Code `protobuf:"varint,11,opt,name=code,proto3,enum=base.Code" json:"code,omitempty"`
}

func (x *PeerResult) Reset() {
	*x = PeerResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerResult) ProtoMessage() {}

func (x *PeerResult) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerResult.ProtoReflect.Descriptor instead.
func (*PeerResult) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{4}
}

func (x *PeerResult) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PeerResult) GetPeerId() string {
	if x != nil {
		return x.PeerId
	}
	return ""
}

func (x *PeerResult) GetSrcIp() string {
	if x != nil {
		return x.SrcIp
	}
	return ""
}

func (x *PeerResult) GetSecurityDomain() string {
	if x != nil {
		return x.SecurityDomain
	}
	return ""
}

func (x *PeerResult) GetIdc() string {
	if x != nil {
		return x.Idc
	}
	return ""
}

func (x *PeerResult) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

func (x *PeerResult) GetContentLength() int64 {
	if x != nil {
		return x.ContentLength
	}
	return 0
}

func (x *PeerResult) GetTraffic() uint64 {
	if x != nil {
		return uint64(x.Traffic)
	}
	return 0
}

func (x *PeerResult) GetCost() uint32 {
	if x != nil {
		return x.Cost
	}
	return 0
}

func (x *PeerResult) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *PeerResult) GetCode() base.Code {
	if x != nil {
		return x.Code
	}
	return base.Code_X_UNSPECIFIED
}

type PeerTarget struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	PeerId string `protobuf:"bytes,2,opt,name=peer_id,json=peerId,proto3" json:"peer_id,omitempty"`
}

func (x *PeerTarget) Reset() {
	*x = PeerTarget{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerTarget) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerTarget) ProtoMessage() {}

func (x *PeerTarget) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_scheduler_scheduler_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerTarget.ProtoReflect.Descriptor instead.
func (*PeerTarget) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP(), []int{5}
}

func (x *PeerTarget) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *PeerTarget) GetPeerId() string {
	if x != nil {
		return x.PeerId
	}
	return ""
}

var File_pkg_rpc_scheduler_scheduler_proto protoreflect.FileDescriptor

var file_pkg_rpc_scheduler_scheduler_proto_rawDesc = []byte{
	0x0a, 0x21, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x72, 0x2f, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x09, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x1a, 0x17,
	0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2f, 0x62, 0x61, 0x73,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xf4, 0x01, 0x0a, 0x0f, 0x50, 0x65, 0x65, 0x72,
	0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x75,
	0x72, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x6c, 0x12, 0x16, 0x0a,
	0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66,
	0x69, 0x6c, 0x74, 0x65, 0x72, 0x12, 0x15, 0x0a, 0x06, 0x62, 0x69, 0x7a, 0x5f, 0x69, 0x64, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x62, 0x69, 0x7a, 0x49, 0x64, 0x12, 0x28, 0x0a, 0x08,
	0x75, 0x72, 0x6c, 0x5f, 0x6d, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d,
	0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x55, 0x72, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x07, 0x75,
	0x72, 0x6c, 0x4d, 0x61, 0x74, 0x61, 0x12, 0x17, 0x0a, 0x07, 0x70, 0x65, 0x65, 0x72, 0x5f, 0x69,
	0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x70, 0x65, 0x65, 0x72, 0x49, 0x64, 0x12,
	0x30, 0x0a, 0x09, 0x70, 0x65, 0x65, 0x72, 0x5f, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x13, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e, 0x50,
	0x65, 0x65, 0x72, 0x48, 0x6f, 0x73, 0x74, 0x52, 0x08, 0x70, 0x65, 0x65, 0x72, 0x48, 0x6f, 0x73,
	0x74, 0x12, 0x2b, 0x0a, 0x09, 0x68, 0x6f, 0x73, 0x74, 0x5f, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x07,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x48, 0x6f, 0x73, 0x74,
	0x4c, 0x6f, 0x61, 0x64, 0x52, 0x08, 0x68, 0x6f, 0x73, 0x74, 0x4c, 0x6f, 0x61, 0x64, 0x22, 0xce,
	0x01, 0x0a, 0x08, 0x50, 0x65, 0x65, 0x72, 0x48, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x75,
	0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75, 0x69, 0x64, 0x12,
	0x0e, 0x0a, 0x02, 0x69, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x70, 0x12,
	0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x70,
	0x6f, 0x72, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x68, 0x6f, 0x73, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x68, 0x6f, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x27, 0x0a, 0x0f, 0x73, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x5f, 0x64, 0x6f, 0x6d,
	0x61, 0x69, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x73, 0x65, 0x63, 0x75, 0x72,
	0x69, 0x74, 0x79, 0x44, 0x6f, 0x6d, 0x61, 0x69, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x6c, 0x6f, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6c, 0x6f, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x10, 0x0a, 0x03, 0x69, 0x64, 0x63, 0x18, 0x07, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x69, 0x64, 0x63, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x77, 0x69, 0x74, 0x63,
	0x68, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x77, 0x69, 0x74, 0x63, 0x68, 0x22,
	0xb3, 0x02, 0x0a, 0x0a, 0x50, 0x65, 0x65, 0x72, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x12, 0x29,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e,
	0x62, 0x61, 0x73, 0x65, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73,
	0x6b, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b,
	0x49, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x73, 0x72, 0x63, 0x5f, 0x70, 0x69, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x72, 0x63, 0x50, 0x69, 0x64, 0x12, 0x25, 0x0a, 0x0e, 0x70,
	0x61, 0x72, 0x61, 0x6c, 0x6c, 0x65, 0x6c, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x0d, 0x70, 0x61, 0x72, 0x61, 0x6c, 0x6c, 0x65, 0x6c, 0x43, 0x6f, 0x75,
	0x6e, 0x74, 0x12, 0x30, 0x0a, 0x09, 0x6d, 0x61, 0x69, 0x6e, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x72, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x48, 0x6f, 0x73, 0x74, 0x52, 0x08, 0x6d, 0x61, 0x69, 0x6e,
	0x50, 0x65, 0x65, 0x72, 0x12, 0x34, 0x0a, 0x0b, 0x73, 0x74, 0x65, 0x61, 0x6c, 0x5f, 0x70, 0x65,
	0x65, 0x72, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x48, 0x6f, 0x73, 0x74, 0x52, 0x0a,
	0x73, 0x74, 0x65, 0x61, 0x6c, 0x50, 0x65, 0x65, 0x72, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x6f,
	0x6e, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x12, 0x25,
	0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x5f, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68,
	0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x4c,
	0x65, 0x6e, 0x67, 0x74, 0x68, 0x22, 0xb7, 0x02, 0x0a, 0x0b, 0x50, 0x69, 0x65, 0x63, 0x65, 0x52,
	0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x17,
	0x0a, 0x07, 0x73, 0x72, 0x63, 0x5f, 0x70, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x73, 0x72, 0x63, 0x50, 0x69, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x64, 0x73, 0x74, 0x5f, 0x70,
	0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x64, 0x73, 0x74, 0x50, 0x69, 0x64,
	0x12, 0x1b, 0x0a, 0x09, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x08, 0x70, 0x69, 0x65, 0x63, 0x65, 0x4e, 0x75, 0x6d, 0x12, 0x1f, 0x0a,
	0x0b, 0x70, 0x69, 0x65, 0x63, 0x65, 0x5f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0a, 0x70, 0x69, 0x65, 0x63, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x1d,
	0x0a, 0x0a, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x09, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x19, 0x0a,
	0x08, 0x65, 0x6e, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x07, 0x65, 0x6e, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63,
	0x65, 0x73, 0x73, 0x18, 0x08, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65,
	0x73, 0x73, 0x12, 0x1e, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x0a, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x63, 0x6f,
	0x64, 0x65, 0x12, 0x2b, 0x0a, 0x09, 0x68, 0x6f, 0x73, 0x74, 0x5f, 0x6c, 0x6f, 0x61, 0x64, 0x18,
	0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x48, 0x6f, 0x73,
	0x74, 0x4c, 0x6f, 0x61, 0x64, 0x52, 0x08, 0x68, 0x6f, 0x73, 0x74, 0x4c, 0x6f, 0x61, 0x64, 0x22,
	0xb1, 0x02, 0x0a, 0x0a, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x17,
	0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x70, 0x65, 0x65, 0x72, 0x5f,
	0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x70, 0x65, 0x65, 0x72, 0x49, 0x64,
	0x12, 0x15, 0x0a, 0x06, 0x73, 0x72, 0x63, 0x5f, 0x69, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x73, 0x72, 0x63, 0x49, 0x70, 0x12, 0x27, 0x0a, 0x0f, 0x73, 0x65, 0x63, 0x75, 0x72,
	0x69, 0x74, 0x79, 0x5f, 0x64, 0x6f, 0x6d, 0x61, 0x69, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0e, 0x73, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x44, 0x6f, 0x6d, 0x61, 0x69, 0x6e,
	0x12, 0x10, 0x0a, 0x03, 0x69, 0x64, 0x63, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x69,
	0x64, 0x63, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x75, 0x72, 0x6c, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x5f,
	0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x63, 0x6f,
	0x6e, 0x74, 0x65, 0x6e, 0x74, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x12, 0x18, 0x0a, 0x07, 0x74,
	0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x74, 0x72,
	0x61, 0x66, 0x66, 0x69, 0x63, 0x12, 0x12, 0x0a, 0x04, 0x63, 0x6f, 0x73, 0x74, 0x18, 0x09, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x04, 0x63, 0x6f, 0x73, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63,
	0x65, 0x73, 0x73, 0x12, 0x1e, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x0b, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x0a, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x63,
	0x6f, 0x64, 0x65, 0x22, 0x3e, 0x0a, 0x0a, 0x50, 0x65, 0x65, 0x72, 0x54, 0x61, 0x72, 0x67, 0x65,
	0x74, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x70, 0x65,
	0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x70, 0x65, 0x65,
	0x72, 0x49, 0x64, 0x32, 0x9b, 0x02, 0x0a, 0x09, 0x53, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x72, 0x12, 0x47, 0x0a, 0x10, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x50, 0x65, 0x65,
	0x72, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x1a, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65,
	0x72, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x15, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e, 0x50, 0x65,
	0x65, 0x72, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x22, 0x00, 0x12, 0x48, 0x0a, 0x11, 0x52, 0x65,
	0x70, 0x6f, 0x72, 0x74, 0x50, 0x69, 0x65, 0x63, 0x65, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12,
	0x16, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e, 0x50, 0x69, 0x65, 0x63,
	0x65, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x1a, 0x15, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75,
	0x6c, 0x65, 0x72, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x22, 0x00,
	0x28, 0x01, 0x30, 0x01, 0x12, 0x40, 0x0a, 0x10, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x50, 0x65,
	0x65, 0x72, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x15, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x72, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x1a,
	0x13, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x53,
	0x74, 0x61, 0x74, 0x65, 0x22, 0x00, 0x12, 0x39, 0x0a, 0x09, 0x4c, 0x65, 0x61, 0x76, 0x65, 0x54,
	0x61, 0x73, 0x6b, 0x12, 0x15, 0x2e, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x2e,
	0x50, 0x65, 0x65, 0x72, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x1a, 0x13, 0x2e, 0x62, 0x61, 0x73,
	0x65, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x22,
	0x00, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x64, 0x72, 0x61, 0x67, 0x6f, 0x6e, 0x66, 0x6c, 0x79, 0x6f, 0x73, 0x73, 0x2f, 0x44, 0x72, 0x61,
	0x67, 0x6f, 0x6e, 0x66, 0x6c, 0x79, 0x32, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f,
	0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_pkg_rpc_scheduler_scheduler_proto_rawDescOnce sync.Once
	file_pkg_rpc_scheduler_scheduler_proto_rawDescData = file_pkg_rpc_scheduler_scheduler_proto_rawDesc
)

func file_pkg_rpc_scheduler_scheduler_proto_rawDescGZIP() []byte {
	file_pkg_rpc_scheduler_scheduler_proto_rawDescOnce.Do(func() {
		file_pkg_rpc_scheduler_scheduler_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_rpc_scheduler_scheduler_proto_rawDescData)
	})
	return file_pkg_rpc_scheduler_scheduler_proto_rawDescData
}

var file_pkg_rpc_scheduler_scheduler_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_pkg_rpc_scheduler_scheduler_proto_goTypes = []interface{}{
	(*PeerTaskRequest)(nil),    // 0: scheduler.PeerTaskRequest
	(*PeerHost)(nil),           // 1: scheduler.PeerHost
	(*PeerPacket)(nil),         // 2: scheduler.PeerPacket
	(*PieceResult)(nil),        // 3: scheduler.PieceResult
	(*PeerResult)(nil),         // 4: scheduler.PeerResult
	(*PeerTarget)(nil),         // 5: scheduler.PeerTarget
	(*base.UrlMeta)(nil),       // 6: base.UrlMeta
	(*base.HostLoad)(nil),      // 7: base.HostLoad
	(*base.ResponseState)(nil), // 8: base.ResponseState
	(base.Code)(0),             // 9: base.Code
}
var file_pkg_rpc_scheduler_scheduler_proto_depIdxs = []int32{
	6,  // 0: scheduler.PeerTaskRequest.url_mata:type_name -> base.UrlMeta
	1,  // 1: scheduler.PeerTaskRequest.peer_host:type_name -> scheduler.PeerHost
	7,  // 2: scheduler.PeerTaskRequest.host_load:type_name -> base.HostLoad
	8,  // 3: scheduler.PeerPacket.state:type_name -> base.ResponseState
	1,  // 4: scheduler.PeerPacket.main_peer:type_name -> scheduler.PeerHost
	1,  // 5: scheduler.PeerPacket.steal_peers:type_name -> scheduler.PeerHost
	9,  // 6: scheduler.PieceResult.code:type_name -> base.Code
	7,  // 7: scheduler.PieceResult.host_load:type_name -> base.HostLoad
	9,  // 8: scheduler.PeerResult.code:type_name -> base.Code
	0,  // 9: scheduler.Scheduler.RegisterPeerTask:input_type -> scheduler.PeerTaskRequest
	3,  // 10: scheduler.Scheduler.ReportPieceResult:input_type -> scheduler.PieceResult
	4,  // 11: scheduler.Scheduler.ReportPeerResult:input_type -> scheduler.PeerResult
	5,  // 12: scheduler.Scheduler.LeaveTask:input_type -> scheduler.PeerTarget
	2,  // 13: scheduler.Scheduler.RegisterPeerTask:output_type -> scheduler.PeerPacket
	2,  // 14: scheduler.Scheduler.ReportPieceResult:output_type -> scheduler.PeerPacket
	8,  // 15: scheduler.Scheduler.ReportPeerResult:output_type -> base.ResponseState
	8,  // 16: scheduler.Scheduler.LeaveTask:output_type -> base.ResponseState
	13, // [13:17] is the sub-list for method output_type
	9,  // [9:13] is the sub-list for method input_type
	9,  // [9:9] is the sub-list for extension type_name
	9,  // [9:9] is the sub-list for extension extendee
	0,  // [0:9] is the sub-list for field type_name
}

func init() { file_pkg_rpc_scheduler_scheduler_proto_init() }
func file_pkg_rpc_scheduler_scheduler_proto_init() {
	if File_pkg_rpc_scheduler_scheduler_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerTaskRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerHost); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerPacket); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PieceResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_rpc_scheduler_scheduler_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerTarget); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_rpc_scheduler_scheduler_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_rpc_scheduler_scheduler_proto_goTypes,
		DependencyIndexes: file_pkg_rpc_scheduler_scheduler_proto_depIdxs,
		MessageInfos:      file_pkg_rpc_scheduler_scheduler_proto_msgTypes,
	}.Build()
	File_pkg_rpc_scheduler_scheduler_proto = out.File
	file_pkg_rpc_scheduler_scheduler_proto_rawDesc = nil
	file_pkg_rpc_scheduler_scheduler_proto_goTypes = nil
	file_pkg_rpc_scheduler_scheduler_proto_depIdxs = nil
}
