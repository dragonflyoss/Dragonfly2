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

package client

import (
	"context"
	"fmt"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
)

var _ DaemonClient = (*daemonClient)(nil)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of dfdaemon is empty")
	}

	resolver := rpc.NewD7yResolver("daemon", addrs)

	dialOpts := append(append(append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy))),
		grpc.WithResolvers(resolver)),
		opts...)

	// "dfdaemon.Daemon" is the dfdaemon._Daemon_serviceDesc.ServiceName
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", "daemon", "dfdaemon.Daemon"),
		dialOpts...,
	)

	if err != nil {
		return nil, err
	}

	cc := &daemonClient{
		cc:           conn,
		daemonClient: dfdaemon.NewDaemonClient(conn),
		resolver:     resolver,
	}
	return cc, nil
}

func GetElasticClientByAddr(addr dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	conn, err := getClientByAddr(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &daemonClient{
		daemonClient: dfdaemon.NewDaemonClient(conn),
		cc:           conn,
	}, nil
}

func getClientByAddr(ctx context.Context, addr dfnet.NetAddr, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOpts := append(append(rpc.DefaultClientOpts, grpc.WithDisableServiceConfig()), opts...)
	return grpc.DialContext(ctx, addr.GetEndpoint(), dialOpts...)
}

// DaemonClient see dfdaemon.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (dfdaemon.Daemon_DownloadClient, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) error

	Close() error
}

type daemonClient struct {
	cc           *grpc.ClientConn
	daemonClient dfdaemon.DaemonClient
	resolver     resolver.Resolver
}

func (dc *daemonClient) getDaemonClient() (dfdaemon.DaemonClient, error) {
	return dc.daemonClient, nil
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (dfdaemon.Daemon_DownloadClient, error) {
	req.Uuid = uuid.New().String()
	// generate taskID
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	return
	dc.daemonClient.Download(ctx, req, opts...)
	return newDownResultStream(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: taskID, Attempt: 1}), dc, taskID, req, opts)
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, target dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket,
	error) {
	ctx = rpc.NewContext(ctx, &rpc.PickRequest{
		TargetAddr: target.String(),
	})
	opts = append([]grpc.CallOption{
		grpc_retry.WithCodes(codes.NotFound, codes.Aborted, codes.Unavailable, codes.Unknown, codes.Internal),
	}, opts...)
	return dc.daemonClient.GetPieceTasks(ctx, ptr, opts...)

}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (err error) {
	ctx = rpc.NewContext(ctx, &rpc.PickRequest{
		TargetAddr: target.String(),
	})
	opts = append([]grpc.CallOption{
		grpc_retry.WithCodes(codes.Unavailable, codes.Unknown, codes.Internal),
	}, opts...)
	_, err = dc.daemonClient.CheckHealth(ctx, new(empty.Empty), opts...)
	return
}

func (dc *daemonClient) Close() error {
	return dc.cc.Close()
}
