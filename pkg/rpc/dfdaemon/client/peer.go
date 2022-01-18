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

//go:generate mockgen -destination ../mocks/peer_mock.go -package mocks d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client ElasticClient

package client

import (
	"context"
	"fmt"
	"strings"

	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func GetElasticClient(opts ...grpc.DialOption) (ElasticClient, error) {
	resolver := rpc.NewD7yResolver("elastic", []dfnet.NetAddr{})

	dialOpts := append(append(append(
		rpc.DefaultClientOpts,
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, rpc.D7yBalancerPolicy))),
		grpc.WithResolvers(resolver)),
		opts...)

	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", "elastic", "elastic.GetPieceTasks"),
		dialOpts...,
	)

	if err != nil {
		return nil, err
	}

	return &elasticClient{
		cc:           conn,
		daemonClient: dfdaemon.NewDaemonClient(conn),
		cdnClient:    cdnsystem.NewSeederClient(conn),
		resolver:     resolver,
	}, nil
}

type elasticClient struct {
	cc           *grpc.ClientConn
	daemonClient dfdaemon.DaemonClient
	cdnClient    cdnsystem.SeederClient
	resolver     *rpc.D7yResolver
}

type ElasticClient interface {
	GetPieceTasks(ctx context.Context, destPeer *scheduler.PeerPacket_DestPeer, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	Close() error
}

func (dc *elasticClient) GetPieceTasks(ctx context.Context, destPeer *scheduler.PeerPacket_DestPeer, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket,
	error) {
	targetAddr := fmt.Sprintf("%s:%d", destPeer.Ip, destPeer.RpcPort)
	if err := dc.resolver.AddAddresses([]dfnet.NetAddr{{
		Addr: targetAddr,
	}}); err != nil {
		return nil, err
	}
	ctx = pickreq.NewContext(ctx, &pickreq.PickRequest{
		TargetAddr: targetAddr,
	})
	peerID := destPeer.PeerId
	toCdn := strings.HasSuffix(peerID, common.CdnSuffix)
	if toCdn {
		return dc.cdnClient.GetPieceTasks(ctx, ptr, opts...)
	}
	return dc.daemonClient.GetPieceTasks(ctx, ptr, opts...)
}

func (dc *elasticClient) Close() error {
	return dc.cc.Close()
}
