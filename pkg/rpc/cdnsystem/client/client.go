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
	"sync"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (CdnClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	cc := &cdnClient{
		rpc.NewConnection(context.Background(), rpc.CDNScheme, addrs, []rpc.ConnOption{
			rpc.WithDialOption(opts),
		}),
	}
	return cc, nil
}

var once sync.Once
var elasticCdnClient *cdnClient

func GetElasticClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (CdnClient, error) {
	once.Do(func() {
		elasticCdnClient = &cdnClient{
			rpc.NewConnection(context.Background(), rpc.CDNElasticScheme, make([]dfnet.NetAddr, 0), []rpc.ConnOption{
				rpc.WithDialOption(opts),
			}),
		}
	})
	err := elasticCdnClient.Connection.AddServerNodes(addrs)
	if err != nil {
		return nil, err
	}
	return elasticCdnClient, nil
}

// CdnClient see cdnsystem.CdnClient
type CdnClient interface {
	ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	UpdateState(addrs []dfnet.NetAddr)

	Close() error
}

type cdnClient struct {
	*rpc.Connection
}

var _ CdnClient = (*cdnClient)(nil)

func (cc *cdnClient) getCdnClient() (cdnsystem.SeederClient, string, error) {
	// "cdnsystem.Seeder" is the cdnsystem._Seeder_serviceDesc.ServiceName
	clientConn, err := cc.Connection.NewClient(fmt.Sprintf("%s:///%s", rpc.CDNScheme, "cdnsystem.Seeder"))
	if err != nil {
		return nil, "", err
	}
	return cdnsystem.NewSeederClient(clientConn), clientConn.Target(), nil
}

func (cc *cdnClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error) {
	return newPieceSeedStream(ctx, cc, sr.TaskId, sr, opts)
}

func (cc *cdnClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		clientConn, err := grpc.Dial(addr.GetEndpoint())
		if err != nil {
			return nil, err
		}
		defer clientConn.Close()
		client := cdnsystem.NewSeederClient(clientConn)
		return client.GetPieceTasks(ctx, req, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskID(req.TaskId).Infof("GetPieceTasks: invoke cdn node %s GetPieceTasks failed: %v", addr.GetEndpoint(), err)
		return nil, err
	}
	return res.(*base.PiecePacket), nil
}
