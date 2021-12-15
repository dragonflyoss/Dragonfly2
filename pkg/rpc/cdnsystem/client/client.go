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

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
)

func GetClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (CdnClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	resolver := rpc.NewD7yResolver(rpc.CDNScheme, addrs)

	dialOpts := append(append(append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy))),
		grpc.WithResolvers(resolver)),
		opts...)

	ctx, cancel := context.WithCancel(context.Background())

	conn, err := grpc.DialContext(
		ctx,
		// "cdnsystem.Seeder" is the cdnsystem._Seeder_serviceDesc.ServiceName
		fmt.Sprintf("%s:///%s", rpc.CDNScheme, "cdnsystem.Seeder"),
		dialOpts...,
	)
	if err != nil {
		cancel()
		return nil, err
	}

	cc := &cdnClient{
		ctx:       ctx,
		cancel:    cancel,
		cdnClient: cdnsystem.NewSeederClient(conn),
		conn:      conn,
		resolver:  resolver,
	}
	return cc, nil
}

func GetElasticClientByAddr(addr dfnet.NetAddr, opts ...grpc.DialOption) (CdnClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := getClientByAddr(ctx, addr, opts...)
	if err != nil {
		cancel()
		return nil, err
	}

	return &cdnClient{
		ctx:       ctx,
		cancel:    cancel,
		cdnClient: cdnsystem.NewSeederClient(conn),
		conn:      conn,
	}, nil
}

func getClientByAddr(ctx context.Context, addr dfnet.NetAddr, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOpts := append(append(rpc.DefaultClientOpts, grpc.WithDisableServiceConfig()), opts...)
	return grpc.DialContext(ctx, addr.GetEndpoint(), dialOpts...)
}

// CdnClient see cdnsystem.CdnClient
type CdnClient interface {
	ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	UpdateState(addrs []dfnet.NetAddr)

	Close() error
}

type cdnClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cdnClient cdnsystem.SeederClient
	conn      *grpc.ClientConn
	resolver  *rpc.D7yResolver
}

var _ CdnClient = (*cdnClient)(nil)

func (cc *cdnClient) getCdnClient() (cdnsystem.SeederClient, error) {
	return cc.cdnClient, nil
}

func (cc *cdnClient) getCdnClientByAddr(addr dfnet.NetAddr) (cdnsystem.SeederClient, error) {
	conn, err := getClientByAddr(cc.ctx, addr)
	if err != nil {
		return nil, err
	}
	return cdnsystem.NewSeederClient(conn), nil
}

func (cc *cdnClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error) {
	return newPieceSeedStream(ctx, cc, sr.TaskId, sr, opts)
}

func (cc *cdnClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, err := cc.getCdnClientByAddr(addr)
		if err != nil {
			return nil, err
		}
		return client.GetPieceTasks(ctx, req, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskID(req.TaskId).Infof("GetPieceTasks: invoke cdn node %s GetPieceTasks failed: %v", addr.GetEndpoint(), err)
		return nil, err
	}
	return res.(*base.PiecePacket), nil
}

func (cc *cdnClient) UpdateState(addrs []dfnet.NetAddr) {
	if err := cc.resolver.UpdateAddrs(addrs); err != nil {
		// TODO(zzy987) modify log
		logger.Errorf("update resolver error: %v\n", err)
	}
}

func (cc *cdnClient) Close() error {
	cc.cancel()
	return nil
}
