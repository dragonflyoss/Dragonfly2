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
	"errors"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"google.golang.org/grpc"
)

var sc *seederClient

var once sync.Once

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SeederClient, error) {
	once.Do(func() {

		sc = &seederClient{
			rpc.NewConnection(context.Background(), "cdn", make([]dfnet.NetAddr, 0), []rpc.ConnOption{
				rpc.WithConnExpireTime(5 * time.Minute),
				rpc.WithDialOption(opts),
			}),
		}
	})
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	err := sc.Connection.AddServerNodes(addrs)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

// see cdnsystem.SeederClient
type SeederClient interface {
	ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest,
		opts ...grpc.CallOption) (*base.PiecePacket, error)
}

type seederClient struct {
	*rpc.Connection
}

func (sc *seederClient) getSeederClient(key string, stick bool) (cdnsystem.SeederClient, string, error) {
	clientConn, err := sc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", err
	}
	return cdnsystem.NewSeederClient(clientConn), clientConn.Target(), nil
}

func (sc *seederClient) getSeederClientWithTarget(target string) (cdnsystem.SeederClient, error) {
	conn, err := sc.Connection.GetClientConnByTarget(target)
	if err != nil {
		return nil, err
	}
	return cdnsystem.NewSeederClient(conn), nil
}

func (sc *seederClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error) {
	return newPieceSeedStream(sc, ctx, sr.TaskId, sr, opts)
}

func (sc *seederClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := sc.getSeederClientWithTarget(addr.GetEndpoint()); err != nil {
			return nil, err
		} else {
			return client.GetPieceTasks(ctx, req, opts...)
		}
	}, 0.2, 2.0, 3, nil)
	if err == nil {
		return res.(*base.PiecePacket), err
	}
	return nil, err
}
