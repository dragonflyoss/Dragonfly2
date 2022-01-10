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
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func GetClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (CDNClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	r := rpc.NewD7yResolver("cdn", addrs)
	dialOpts := append(append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy)),
		grpc.WithResolvers(r)),
		opts...)

	clientConn, err := grpc.Dial(
		// "cdnsystem.Seeder" is the cdnsystem._Seeder_serviceDesc.ServiceName
		fmt.Sprintf("%s:///%s", "cdn", "cdnsystem.Seeder"),
		dialOpts...)
	if err != nil {
		return nil, err
	}

	return &cdnClient{
		cc:           clientConn,
		seederClient: cdnsystem.NewSeederClient(clientConn),
		resolver:     r,
	}, nil
}

type CDNClient interface {
	ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, opts ...grpc.CallOption) (cdnsystem.Seeder_ObtainSeedsClient, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	UpdateState(addrs []dfnet.NetAddr)

	Close() error
}

type cdnClient struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cc           *grpc.ClientConn
	seederClient cdnsystem.SeederClient
	resolver     resolver.Resolver
}

var _ CDNClient = (*cdnClient)(nil)

func (cc *cdnClient) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, opts ...grpc.CallOption) (cdnsystem.Seeder_ObtainSeedsClient, error) {
	var temp peer.Peer

	seeder, err := cc.seederClient.ObtainSeeds(ctx, req, append(opts, grpc.Peer(&temp))...)
	if err == nil {
		return seeder, err
	}
	_, err = seeder.Recv()
	rpc.FromContext(ctx)
	status.FromContextError(err)
	// try next CDN
	return nil, nil
}

func (cc *cdnClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	ctx = rpc.NewContext(ctx, &rpc.PickRequest{
		TargetAddr: addr.String(),
	})
	return cc.seederClient.GetPieceTasks(ctx, req, opts...)
}

func (cc *cdnClient) UpdateState(addrs []dfnet.NetAddr) {

}

func (cc *cdnClient) Close() error {
	return cc.cc.Close()
}

func getClientByAddr(ctx context.Context, addr dfnet.NetAddr, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOpts := append(append(rpc.DefaultClientOpts, grpc.WithDisableServiceConfig()), opts...)
	return grpc.DialContext(ctx, addr.GetEndpoint(), dialOpts...)
}

func (cc *cdnClient) getCdnClientByAddr(addr dfnet.NetAddr) (cdnsystem.SeederClient, error) {
	conn, err := getClientByAddr(context.Background(), addr)
	if err != nil {
		return nil, err
	}
	return cdnsystem.NewSeederClient(conn), nil
}
