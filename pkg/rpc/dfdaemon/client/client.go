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
	"time"

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var _ DaemonClient = (*daemonClient)(nil)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of daemon is empty")
	}
	dc := &daemonClient{
		rpc.NewConnection(context.Background(), "daemon-static", addrs, []rpc.ConnOption{
			rpc.WithConnExpireTime(60 * time.Second),
			rpc.WithDialOption(opts),
		}),
	}
	return dc, nil
}

var once sync.Once
var elasticDaemonClient *daemonClient

func GetElasticClientByAddrs(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	once.Do(func() {
		elasticDaemonClient = &daemonClient{
			rpc.NewConnection(context.Background(), "daemon-elastic", make([]dfnet.NetAddr, 0), []rpc.ConnOption{
				rpc.WithConnExpireTime(30 * time.Second),
				rpc.WithDialOption(opts),
			}),
		}
	})
	err := elasticDaemonClient.Connection.AddServerNodes(addrs)
	if err != nil {
		return nil, err
	}
	return elasticDaemonClient, nil
}

// see dfdaemon.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (*DownResultStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) error

	Close() error
}

type daemonClient struct {
	*rpc.Connection
}

func (dc *daemonClient) getDaemonClient(key string, stick bool) (dfdaemon.DaemonClient, string, error) {
	clientConn, err := dc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", err
	}
	return dfdaemon.NewDaemonClient(clientConn), clientConn.Target(), nil
}

func (dc *daemonClient) getDaemonClientWithTarget(target string) (dfdaemon.DaemonClient, error) {
	conn, err := dc.Connection.GetClientConnByTarget(target)
	if err != nil {
		return nil, err
	}
	return dfdaemon.NewDaemonClient(conn), nil
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (*DownResultStream, error) {
	req.Uuid = uuid.New().String()
	// generate taskID
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	return newDownResultStream(ctx, dc, taskID, req, opts)
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, target dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket,
	error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, err := dc.getDaemonClientWithTarget(target.GetEndpoint())
		if err != nil {
			return nil, err
		}
		return client.GetPieceTasks(ctx, ptr, opts...)
	}, 0.2, 2.0, 3, nil)

	if err == nil {
		return res.(*base.PiecePacket), nil
	}

	return nil, err
}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (err error) {
	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, err := dc.getDaemonClientWithTarget(target.GetEndpoint())
		if err != nil {
			return nil, fmt.Errorf("failed to connect server %s: %v", target.GetEndpoint(), err)
		}
		return client.CheckHealth(ctx, new(empty.Empty), opts...)
	}, 0.2, 2.0, 3, nil)

	return
}
