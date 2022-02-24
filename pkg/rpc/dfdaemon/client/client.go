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

//go:generate mockgen -destination ./mocks/mock_client.go -package mocks d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client DaemonClient

package client

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
)

var _ DaemonClient = (*daemonClient)(nil)

func GetClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
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
	resolver     *rpc.D7yResolver
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (dfdaemon.Daemon_DownloadClient, error) {
	if req.Uuid == "" {
		req.Uuid = uuid.New().String()
	}
	// generate taskID
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		HashKey: taskID,
	})
	return dc.daemonClient.Download(ctx, req, opts...)
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, target dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket,
	error) {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		TargetAddr: target.String(),
	})
	return dc.daemonClient.GetPieceTasks(ctx, ptr, opts...)
}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (err error) {
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		TargetAddr: target.String(),
	})
	_, err = dc.daemonClient.CheckHealth(ctx, new(empty.Empty), opts...)
	return
}

func (dc *daemonClient) Close() error {
	return dc.cc.Close()
}
