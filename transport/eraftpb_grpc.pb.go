// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: transport/eraftpb.proto

package transport

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	RaftService_SendMessage_FullMethodName    = "/transport.RaftService/SendMessage"
	RaftService_StreamMessage_FullMethodName  = "/transport.RaftService/StreamMessage"
	RaftService_Heartbeat_FullMethodName      = "/transport.RaftService/Heartbeat"
	RaftService_SendSnapshot_FullMethodName   = "/transport.RaftService/SendSnapshot"
	RaftService_GetLeaderInfo_FullMethodName  = "/transport.RaftService/GetLeaderInfo"
	RaftService_SendConfChange_FullMethodName = "/transport.RaftService/SendConfChange"
)

// RaftServiceClient is the client API for RaftService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// RaftService 定义了 Raft 节点间通信的 gRPC 服务
type RaftServiceClient interface {
	// SendMessage 用于发送单个 Raft 消息
	SendMessage(ctx context.Context, in *RaftMessageRequest, opts ...grpc.CallOption) (*RaftMessageResponse, error)
	// StreamMessage 用于建立双向流式消息传输
	StreamMessage(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[RaftMessageRequest, RaftMessageResponse], error)
	// Heartbeat 用于节点心跳
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	// SendSnapshot 用于传输快照数据块
	SendSnapshot(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SnapshotChunk, SnapshotResponse], error)
	// GetLeaderInfo 获取领导者信息
	GetLeaderInfo(ctx context.Context, in *GetLeaderInfoRequest, opts ...grpc.CallOption) (*GetLeaderInfoResponse, error)
	// SendConfChange 发送配置变更请求
	SendConfChange(ctx context.Context, in *ConfChangeRequest, opts ...grpc.CallOption) (*ConfChangeResponse, error)
}

type raftServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftServiceClient(cc grpc.ClientConnInterface) RaftServiceClient {
	return &raftServiceClient{cc}
}

func (c *raftServiceClient) SendMessage(ctx context.Context, in *RaftMessageRequest, opts ...grpc.CallOption) (*RaftMessageResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RaftMessageResponse)
	err := c.cc.Invoke(ctx, RaftService_SendMessage_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) StreamMessage(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[RaftMessageRequest, RaftMessageResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &RaftService_ServiceDesc.Streams[0], RaftService_StreamMessage_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RaftMessageRequest, RaftMessageResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type RaftService_StreamMessageClient = grpc.BidiStreamingClient[RaftMessageRequest, RaftMessageResponse]

func (c *raftServiceClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, RaftService_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) SendSnapshot(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[SnapshotChunk, SnapshotResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &RaftService_ServiceDesc.Streams[1], RaftService_SendSnapshot_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SnapshotChunk, SnapshotResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type RaftService_SendSnapshotClient = grpc.BidiStreamingClient[SnapshotChunk, SnapshotResponse]

func (c *raftServiceClient) GetLeaderInfo(ctx context.Context, in *GetLeaderInfoRequest, opts ...grpc.CallOption) (*GetLeaderInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetLeaderInfoResponse)
	err := c.cc.Invoke(ctx, RaftService_GetLeaderInfo_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) SendConfChange(ctx context.Context, in *ConfChangeRequest, opts ...grpc.CallOption) (*ConfChangeResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ConfChangeResponse)
	err := c.cc.Invoke(ctx, RaftService_SendConfChange_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServiceServer is the server API for RaftService service.
// All implementations must embed UnimplementedRaftServiceServer
// for forward compatibility.
//
// RaftService 定义了 Raft 节点间通信的 gRPC 服务
type RaftServiceServer interface {
	// SendMessage 用于发送单个 Raft 消息
	SendMessage(context.Context, *RaftMessageRequest) (*RaftMessageResponse, error)
	// StreamMessage 用于建立双向流式消息传输
	StreamMessage(grpc.BidiStreamingServer[RaftMessageRequest, RaftMessageResponse]) error
	// Heartbeat 用于节点心跳
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// SendSnapshot 用于传输快照数据块
	SendSnapshot(grpc.BidiStreamingServer[SnapshotChunk, SnapshotResponse]) error
	// GetLeaderInfo 获取领导者信息
	GetLeaderInfo(context.Context, *GetLeaderInfoRequest) (*GetLeaderInfoResponse, error)
	// SendConfChange 发送配置变更请求
	SendConfChange(context.Context, *ConfChangeRequest) (*ConfChangeResponse, error)
	mustEmbedUnimplementedRaftServiceServer()
}

// UnimplementedRaftServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRaftServiceServer struct{}

func (UnimplementedRaftServiceServer) SendMessage(context.Context, *RaftMessageRequest) (*RaftMessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendMessage not implemented")
}
func (UnimplementedRaftServiceServer) StreamMessage(grpc.BidiStreamingServer[RaftMessageRequest, RaftMessageResponse]) error {
	return status.Errorf(codes.Unimplemented, "method StreamMessage not implemented")
}
func (UnimplementedRaftServiceServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedRaftServiceServer) SendSnapshot(grpc.BidiStreamingServer[SnapshotChunk, SnapshotResponse]) error {
	return status.Errorf(codes.Unimplemented, "method SendSnapshot not implemented")
}
func (UnimplementedRaftServiceServer) GetLeaderInfo(context.Context, *GetLeaderInfoRequest) (*GetLeaderInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLeaderInfo not implemented")
}
func (UnimplementedRaftServiceServer) SendConfChange(context.Context, *ConfChangeRequest) (*ConfChangeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SendConfChange not implemented")
}
func (UnimplementedRaftServiceServer) mustEmbedUnimplementedRaftServiceServer() {}
func (UnimplementedRaftServiceServer) testEmbeddedByValue()                     {}

// UnsafeRaftServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftServiceServer will
// result in compilation errors.
type UnsafeRaftServiceServer interface {
	mustEmbedUnimplementedRaftServiceServer()
}

func RegisterRaftServiceServer(s grpc.ServiceRegistrar, srv RaftServiceServer) {
	// If the following call pancis, it indicates UnimplementedRaftServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RaftService_ServiceDesc, srv)
}

func _RaftService_SendMessage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RaftMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).SendMessage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_SendMessage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).SendMessage(ctx, req.(*RaftMessageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_StreamMessage_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RaftServiceServer).StreamMessage(&grpc.GenericServerStream[RaftMessageRequest, RaftMessageResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type RaftService_StreamMessageServer = grpc.BidiStreamingServer[RaftMessageRequest, RaftMessageResponse]

func _RaftService_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_SendSnapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RaftServiceServer).SendSnapshot(&grpc.GenericServerStream[SnapshotChunk, SnapshotResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type RaftService_SendSnapshotServer = grpc.BidiStreamingServer[SnapshotChunk, SnapshotResponse]

func _RaftService_GetLeaderInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetLeaderInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).GetLeaderInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_GetLeaderInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).GetLeaderInfo(ctx, req.(*GetLeaderInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_SendConfChange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfChangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).SendConfChange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_SendConfChange_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).SendConfChange(ctx, req.(*ConfChangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RaftService_ServiceDesc is the grpc.ServiceDesc for RaftService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RaftService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "transport.RaftService",
	HandlerType: (*RaftServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SendMessage",
			Handler:    _RaftService_SendMessage_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _RaftService_Heartbeat_Handler,
		},
		{
			MethodName: "GetLeaderInfo",
			Handler:    _RaftService_GetLeaderInfo_Handler,
		},
		{
			MethodName: "SendConfChange",
			Handler:    _RaftService_SendConfChange_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamMessage",
			Handler:       _RaftService_StreamMessage_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "SendSnapshot",
			Handler:       _RaftService_SendSnapshot_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "transport/eraftpb.proto",
}
