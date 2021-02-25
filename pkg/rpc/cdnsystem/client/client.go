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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/safe"
	"errors"
	"google.golang.org/grpc"
)

var cdnClient SeederClient

func GetClient() (SeederClient, error) {
	// 从本地文件/manager读取addrs
	return newCdnClient([]dfnet.NetAddr{})
}

func GetClientByAddr(addr []dfnet.NetAddr) (SeederClient, error) {
	return newCdnClient(addr)
}

func newCdnClient(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SeederClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	return &seederClient{
		rpc.NewConnection(addrs, opts...),
	}, nil
}

// see cdnsystem.SeederClient
type SeederClient interface {
	ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (<-chan *cdnsystem.PieceSeed, error)

	GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)
}

type seederClient struct {
	*rpc.Connection
}

func (sc *seederClient) getSeederClient(key string) cdnsystem.SeederClient {
	return cdnsystem.NewSeederClient(sc.Connection.GetClientConn(key))
}

func (sc *seederClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (<-chan *cdnsystem.PieceSeed, error) {
	psc := make(chan *cdnsystem.PieceSeed, 4)
	pss, err := newPieceSeedStream(sc, ctx, sr.TaskId, sr, opts)
	if err != nil {
		return nil, err
	}

	go receive(pss, psc)

	return psc, nil
}

func (sc *seederClient) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return sc.getSeederClient(req.TaskId).GetPieceTasks(ctx, req, opts...)
	}, 0.2, 2.0, 3, nil)

	if err == nil {
		return res.(*base.PiecePacket), nil
	}

	return nil, err
}

func receive(pss *pieceSeedStream, psc chan *cdnsystem.PieceSeed) {
	safe.Call(func() {
		defer close(psc)
		for {
			pieceSeed, err := pss.recv()
			if err == nil {
				psc <- pieceSeed
				if pieceSeed.Done {
					return
				}
			} else {
				psc <- common.NewResWithErr(pieceSeed, err).(*cdnsystem.PieceSeed)
				return
			}
		}
	})
}
