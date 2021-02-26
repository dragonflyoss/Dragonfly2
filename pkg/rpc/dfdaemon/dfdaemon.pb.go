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
// source: pkg/rpc/dfdaemon/dfdaemon.proto

package dfdaemon

import (
	base "d7y.io/dragonfly/v2/pkg/rpc/base"
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

type DownRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// download content with the url, but not only for http protocol
	Url string `protobuf:"bytes,1,opt,name=url,proto3" json:"url,omitempty"`
	// pieces will be written to output path directly,
	// at the same time, dfdaemon workspace also makes soft link to the output
	Output  string        `protobuf:"bytes,2,opt,name=output,proto3" json:"output,omitempty"`
	UrlMeta *base.UrlMeta `protobuf:"bytes,3,opt,name=url_meta,json=urlMeta,proto3" json:"url_meta,omitempty"`
	// caller business id
	BizId string `protobuf:"bytes,4,opt,name=biz_id,json=bizId,proto3" json:"biz_id,omitempty"`
	// regex format
	Filter string `protobuf:"bytes,5,opt,name=filter,proto3" json:"filter,omitempty"`
	// identify one downloading
	// framework will fill it automatically
	Uuid string `protobuf:"bytes,6,opt,name=uuid,proto3" json:"uuid,omitempty"`
}

func (x *DownRequest) Reset() {
	*x = DownRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DownRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DownRequest) ProtoMessage() {}

func (x *DownRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DownRequest.ProtoReflect.Descriptor instead.
func (*DownRequest) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescGZIP(), []int{0}
}

func (x *DownRequest) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

func (x *DownRequest) GetOutput() string {
	if x != nil {
		return x.Output
	}
	return ""
}

func (x *DownRequest) GetUrlMeta() *base.UrlMeta {
	if x != nil {
		return x.UrlMeta
	}
	return nil
}

func (x *DownRequest) GetBizId() string {
	if x != nil {
		return x.BizId
	}
	return ""
}

func (x *DownRequest) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

func (x *DownRequest) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

type DownResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	State           *base.ResponseState `protobuf:"bytes,1,opt,name=state,proto3" json:"state,omitempty"`
	TaskId          string              `protobuf:"bytes,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	PeerId          string              `protobuf:"bytes,3,opt,name=peer_id,json=peerId,proto3" json:"peer_id,omitempty"`
	CompletedLength uint64              `protobuf:"varint,4,opt,name=completed_length,json=completedLength,proto3" json:"completed_length,omitempty"`
	// done with success or fail
	Done bool `protobuf:"varint,5,opt,name=done,proto3" json:"done,omitempty"`
}

func (x *DownResult) Reset() {
	*x = DownResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DownResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DownResult) ProtoMessage() {}

func (x *DownResult) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DownResult.ProtoReflect.Descriptor instead.
func (*DownResult) Descriptor() ([]byte, []int) {
	return file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescGZIP(), []int{1}
}

func (x *DownResult) GetState() *base.ResponseState {
	if x != nil {
		return x.State
	}
	return nil
}

func (x *DownResult) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *DownResult) GetPeerId() string {
	if x != nil {
		return x.PeerId
	}
	return ""
}

func (x *DownResult) GetCompletedLength() uint64 {
	if x != nil {
		return x.CompletedLength
	}
	return 0
}

func (x *DownResult) GetDone() bool {
	if x != nil {
		return x.Done
	}
	return false
}

var File_pkg_rpc_dfdaemon_dfdaemon_proto protoreflect.FileDescriptor

var file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x70, 0x6b, 0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x64, 0x66, 0x64, 0x61, 0x65, 0x6d,
	0x6f, 0x6e, 0x2f, 0x64, 0x66, 0x64, 0x61, 0x65, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x08, 0x64, 0x66, 0x64, 0x61, 0x65, 0x6d, 0x6f, 0x6e, 0x1a, 0x17, 0x70, 0x6b, 0x67,
	0x2f, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa4, 0x01, 0x0a, 0x0b, 0x44, 0x6f, 0x77, 0x6e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x75, 0x72, 0x6c, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x12, 0x28,
	0x0a, 0x08, 0x75, 0x72, 0x6c, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0d, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x55, 0x72, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x52,
	0x07, 0x75, 0x72, 0x6c, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x15, 0x0a, 0x06, 0x62, 0x69, 0x7a, 0x5f,
	0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x62, 0x69, 0x7a, 0x49, 0x64, 0x12,
	0x16, 0x0a, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75, 0x69, 0x64, 0x22, 0xa8, 0x01, 0x0a, 0x0a,
	0x44, 0x6f, 0x77, 0x6e, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x29, 0x0a, 0x05, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x62, 0x61, 0x73, 0x65,
	0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05,
	0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x17,
	0x0a, 0x07, 0x70, 0x65, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x70, 0x65, 0x65, 0x72, 0x49, 0x64, 0x12, 0x29, 0x0a, 0x10, 0x63, 0x6f, 0x6d, 0x70, 0x6c,
	0x65, 0x74, 0x65, 0x64, 0x5f, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x0f, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x4c, 0x65, 0x6e, 0x67,
	0x74, 0x68, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x04, 0x64, 0x6f, 0x6e, 0x65, 0x32, 0xbd, 0x01, 0x0a, 0x06, 0x44, 0x61, 0x65, 0x6d, 0x6f,
	0x6e, 0x12, 0x3b, 0x0a, 0x08, 0x44, 0x6f, 0x77, 0x6e, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x15, 0x2e,
	0x64, 0x66, 0x64, 0x61, 0x65, 0x6d, 0x6f, 0x6e, 0x2e, 0x44, 0x6f, 0x77, 0x6e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x64, 0x66, 0x64, 0x61, 0x65, 0x6d, 0x6f, 0x6e, 0x2e,
	0x44, 0x6f, 0x77, 0x6e, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x22, 0x00, 0x30, 0x01, 0x12, 0x3c,
	0x0a, 0x0d, 0x47, 0x65, 0x74, 0x50, 0x69, 0x65, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x12,
	0x16, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x50, 0x69, 0x65, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x50,
	0x69, 0x65, 0x63, 0x65, 0x50, 0x61, 0x63, 0x6b, 0x65, 0x74, 0x22, 0x00, 0x12, 0x38, 0x0a, 0x0b,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x12, 0x12, 0x2e, 0x62, 0x61,
	0x73, 0x65, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x13, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x53,
	0x74, 0x61, 0x74, 0x65, 0x22, 0x00, 0x42, 0x26, 0x5a, 0x24, 0x64, 0x37, 0x79, 0x2e, 0x69, 0x6f,
	0x2f, 0x64, 0x72, 0x61, 0x67, 0x6f, 0x6e, 0x66, 0x6c, 0x79, 0x2f, 0x76, 0x32, 0x2f, 0x70, 0x6b,
	0x67, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x64, 0x66, 0x64, 0x61, 0x65, 0x6d, 0x6f, 0x6e, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescOnce sync.Once
	file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescData = file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDesc
)

func file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescGZIP() []byte {
	file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescOnce.Do(func() {
		file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescData)
	})
	return file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDescData
}

var file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_rpc_dfdaemon_dfdaemon_proto_goTypes = []interface{}{
	(*DownRequest)(nil),           // 0: dfdaemon.DownRequest
	(*DownResult)(nil),            // 1: dfdaemon.DownResult
	(*base.UrlMeta)(nil),          // 2: base.UrlMeta
	(*base.ResponseState)(nil),    // 3: base.ResponseState
	(*base.PieceTaskRequest)(nil), // 4: base.PieceTaskRequest
	(*base.EmptyRequest)(nil),     // 5: base.EmptyRequest
	(*base.PiecePacket)(nil),      // 6: base.PiecePacket
}
var file_pkg_rpc_dfdaemon_dfdaemon_proto_depIdxs = []int32{
	2, // 0: dfdaemon.DownRequest.url_meta:type_name -> base.UrlMeta
	3, // 1: dfdaemon.DownResult.state:type_name -> base.ResponseState
	0, // 2: dfdaemon.Daemon.Download:input_type -> dfdaemon.DownRequest
	4, // 3: dfdaemon.Daemon.GetPieceTasks:input_type -> base.PieceTaskRequest
	5, // 4: dfdaemon.Daemon.CheckHealth:input_type -> base.EmptyRequest
	1, // 5: dfdaemon.Daemon.Download:output_type -> dfdaemon.DownResult
	6, // 6: dfdaemon.Daemon.GetPieceTasks:output_type -> base.PiecePacket
	3, // 7: dfdaemon.Daemon.CheckHealth:output_type -> base.ResponseState
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_pkg_rpc_dfdaemon_dfdaemon_proto_init() }
func file_pkg_rpc_dfdaemon_dfdaemon_proto_init() {
	if File_pkg_rpc_dfdaemon_dfdaemon_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DownRequest); i {
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
		file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DownResult); i {
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
			RawDescriptor: file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_rpc_dfdaemon_dfdaemon_proto_goTypes,
		DependencyIndexes: file_pkg_rpc_dfdaemon_dfdaemon_proto_depIdxs,
		MessageInfos:      file_pkg_rpc_dfdaemon_dfdaemon_proto_msgTypes,
	}.Build()
	File_pkg_rpc_dfdaemon_dfdaemon_proto = out.File
	file_pkg_rpc_dfdaemon_dfdaemon_proto_rawDesc = nil
	file_pkg_rpc_dfdaemon_dfdaemon_proto_goTypes = nil
	file_pkg_rpc_dfdaemon_dfdaemon_proto_depIdxs = nil
}
