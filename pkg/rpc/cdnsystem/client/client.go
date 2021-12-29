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

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
)

func Dial(target string, opts ...grpc.DialOption) (CDNClient, error) {
	return DialContext(context.Background(), target, opts...)
}

func DialContext(ctx context.Context, target string, opts ...grpc.DialOption) (CDNClient, error) {
	dialOpts := append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy)),
		grpc.WithResolvers(rpc.NewD7yResolverBuilder("cdn")))
	clientConn, err := grpc.DialContext(ctx, target, append(dialOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	return &cdnClient{
		cc:           clientConn,
		seederClient: cdnsystem.NewSeederClient(clientConn),
	}, nil
}

type CDNClient interface {
	ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, opts ...grpc.CallOption) (cdnsystem.Seeder_ObtainSeedsClient, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	Close() error
}

type cdnClient struct {
	cc           *grpc.ClientConn
	seederClient cdnsystem.SeederClient
}

var _ CDNClient = (*cdnClient)(nil)

func (cc *cdnClient) ObtainSeeds(ctx context.Context, req *cdnsystem.SeedRequest, opts ...grpc.CallOption) (cdnsystem.Seeder_ObtainSeedsClient, error) {
	ctx = context.WithValue(ctx, rpc.HashKey, rpc.PickerReq{
		HashKey: req.TaskId,
		Attempt: 0,
	})
	seeder, err := cc.seederClient.ObtainSeeds(ctx, req, opts...)
	if err == nil {
		return seeder, err
	}
	rpc.TryMigrate(ctx, err)
	cc.seederClient.ObtainSeeds(ctx)
	status.FromContextError(err)
	// try next CDN
	return nil, nil
}

func (cc *cdnClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	return cc.seederClient.GetPieceTasks(ctx, req, opts...)
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
