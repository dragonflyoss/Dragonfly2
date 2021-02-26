// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package scheduler

import (
	context "context"
	base "d7y.io/dragonfly/v2/pkg/rpc/base"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion7

// SchedulerClient is the client API for Scheduler service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SchedulerClient interface {
	// RegisterPeerTask registers a peer into one task.
	RegisterPeerTask(ctx context.Context, in *PeerTaskRequest, opts ...grpc.CallOption) (*RegisterResult, error)
	// ReportPieceResult reports piece results and receives peer packets.
	// when migrating to another scheduler,
	// it will send the last piece result to the new scheduler.
	ReportPieceResult(ctx context.Context, opts ...grpc.CallOption) (Scheduler_ReportPieceResultClient, error)
	// ReportPeerResult reports downloading result for the peer task.
	ReportPeerResult(ctx context.Context, in *PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)
	// LeaveTask makes the peer leaving from scheduling overlay for the task.
	LeaveTask(ctx context.Context, in *PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
}

type schedulerClient struct {
	cc grpc.ClientConnInterface
}

func NewSchedulerClient(cc grpc.ClientConnInterface) SchedulerClient {
	return &schedulerClient{cc}
}

func (c *schedulerClient) RegisterPeerTask(ctx context.Context, in *PeerTaskRequest, opts ...grpc.CallOption) (*RegisterResult, error) {
	out := new(RegisterResult)
	err := c.cc.Invoke(ctx, "/scheduler.Scheduler/RegisterPeerTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerClient) ReportPieceResult(ctx context.Context, opts ...grpc.CallOption) (Scheduler_ReportPieceResultClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Scheduler_serviceDesc.Streams[0], "/scheduler.Scheduler/ReportPieceResult", opts...)
	if err != nil {
		return nil, err
	}
	x := &schedulerReportPieceResultClient{stream}
	return x, nil
}

type Scheduler_ReportPieceResultClient interface {
	Send(*PieceResult) error
	Recv() (*PeerPacket, error)
	grpc.ClientStream
}

type schedulerReportPieceResultClient struct {
	grpc.ClientStream
}

func (x *schedulerReportPieceResultClient) Send(m *PieceResult) error {
	return x.ClientStream.SendMsg(m)
}

func (x *schedulerReportPieceResultClient) Recv() (*PeerPacket, error) {
	m := new(PeerPacket)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *schedulerClient) ReportPeerResult(ctx context.Context, in *PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
	out := new(base.ResponseState)
	err := c.cc.Invoke(ctx, "/scheduler.Scheduler/ReportPeerResult", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerClient) LeaveTask(ctx context.Context, in *PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error) {
	out := new(base.ResponseState)
	err := c.cc.Invoke(ctx, "/scheduler.Scheduler/LeaveTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SchedulerServer is the server API for Scheduler service.
// All implementations must embed UnimplementedSchedulerServer
// for forward compatibility
type SchedulerServer interface {
	// RegisterPeerTask registers a peer into one task.
	RegisterPeerTask(context.Context, *PeerTaskRequest) (*RegisterResult, error)
	// ReportPieceResult reports piece results and receives peer packets.
	// when migrating to another scheduler,
	// it will send the last piece result to the new scheduler.
	ReportPieceResult(Scheduler_ReportPieceResultServer) error
	// ReportPeerResult reports downloading result for the peer task.
	ReportPeerResult(context.Context, *PeerResult) (*base.ResponseState, error)
	// LeaveTask makes the peer leaving from scheduling overlay for the task.
	LeaveTask(context.Context, *PeerTarget) (*base.ResponseState, error)
	mustEmbedUnimplementedSchedulerServer()
}

// UnimplementedSchedulerServer must be embedded to have forward compatible implementations.
type UnimplementedSchedulerServer struct {
}

func (UnimplementedSchedulerServer) RegisterPeerTask(context.Context, *PeerTaskRequest) (*RegisterResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterPeerTask not implemented")
}
func (UnimplementedSchedulerServer) ReportPieceResult(Scheduler_ReportPieceResultServer) error {
	return status.Errorf(codes.Unimplemented, "method ReportPieceResult not implemented")
}
func (UnimplementedSchedulerServer) ReportPeerResult(context.Context, *PeerResult) (*base.ResponseState, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportPeerResult not implemented")
}
func (UnimplementedSchedulerServer) LeaveTask(context.Context, *PeerTarget) (*base.ResponseState, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaveTask not implemented")
}
func (UnimplementedSchedulerServer) mustEmbedUnimplementedSchedulerServer() {}

// UnsafeSchedulerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SchedulerServer will
// result in compilation errors.
type UnsafeSchedulerServer interface {
	mustEmbedUnimplementedSchedulerServer()
}

func RegisterSchedulerServer(s grpc.ServiceRegistrar, srv SchedulerServer) {
	s.RegisterService(&_Scheduler_serviceDesc, srv)
}

func _Scheduler_RegisterPeerTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PeerTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServer).RegisterPeerTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler.Scheduler/RegisterPeerTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServer).RegisterPeerTask(ctx, req.(*PeerTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Scheduler_ReportPieceResult_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SchedulerServer).ReportPieceResult(&schedulerReportPieceResultServer{stream})
}

type Scheduler_ReportPieceResultServer interface {
	Send(*PeerPacket) error
	Recv() (*PieceResult, error)
	grpc.ServerStream
}

type schedulerReportPieceResultServer struct {
	grpc.ServerStream
}

func (x *schedulerReportPieceResultServer) Send(m *PeerPacket) error {
	return x.ServerStream.SendMsg(m)
}

func (x *schedulerReportPieceResultServer) Recv() (*PieceResult, error) {
	m := new(PieceResult)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Scheduler_ReportPeerResult_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PeerResult)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServer).ReportPeerResult(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler.Scheduler/ReportPeerResult",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServer).ReportPeerResult(ctx, req.(*PeerResult))
	}
	return interceptor(ctx, in, info, handler)
}

func _Scheduler_LeaveTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PeerTarget)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServer).LeaveTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler.Scheduler/LeaveTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServer).LeaveTask(ctx, req.(*PeerTarget))
	}
	return interceptor(ctx, in, info, handler)
}

var _Scheduler_serviceDesc = grpc.ServiceDesc{
	ServiceName: "scheduler.Scheduler",
	HandlerType: (*SchedulerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegisterPeerTask",
			Handler:    _Scheduler_RegisterPeerTask_Handler,
		},
		{
			MethodName: "ReportPeerResult",
			Handler:    _Scheduler_ReportPeerResult_Handler,
		},
		{
			MethodName: "LeaveTask",
			Handler:    _Scheduler_LeaveTask_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ReportPieceResult",
			Handler:       _Scheduler_ReportPieceResult_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "pkg/rpc/scheduler/scheduler.proto",
}
