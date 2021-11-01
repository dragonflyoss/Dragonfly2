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
		rpc.NewConnection(context.Background(), rpc.DaemonScheme, addrs, []rpc.ConnOption{
			rpc.WithDialOption(opts),
		}),
	}
	return dc, nil
}

var once sync.Once
var elasticDaemonClient *daemonClient

func GetElasticClientByAddr(addr dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	once.Do(func() {
		elasticDaemonClient = &daemonClient{
			rpc.NewConnection(context.Background(), rpc.DaemonScheme, nil, []rpc.ConnOption{
				rpc.WithDialOption(opts),
			}),
		}
	})
	elasticDaemonClient.AddServerNode(addr)
	return elasticDaemonClient, nil
}

// DaemonClient see dfdaemon.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (*DownResultStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) error

	Close() error
}

type daemonClient struct {
	*rpc.Connection
}

func (dc *daemonClient) getDaemonClient() (dfdaemon.DaemonClient, error) {
	// "dfdaemon.Daemon" is the dfdaemon._Daemon_serviceDesc.ServiceName
	clientConn, err := dc.Connection.GetConsistentHashClient(fmt.Sprintf("%s:///%s", rpc.DaemonScheme, "dfdaemon.Daemon"))
	if err != nil {
		return nil, err
	}
	return dfdaemon.NewDaemonClient(clientConn), nil
}

func (dc *daemonClient) getDaemonClientByTarget(target string) (dfdaemon.DaemonClient, error) {
	clientConn, err := dc.Connection.GetDirectClient(target)
	if err != nil {
		return nil, err
	}
	return dfdaemon.NewDaemonClient(clientConn), nil
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
		client, err := dc.getDaemonClientByTarget(target.GetEndpoint())
		if err != nil {
			return nil, err
		}
		return client.GetPieceTasks(ctx, ptr, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskID(ptr.TaskId).Infof("GetPieceTasks: invoke daemon node %s GetPieceTasks failed: %v", target, err)
		return nil, err
	}
	return res.(*base.PiecePacket), nil
}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (err error) {
	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, err := dc.getDaemonClientByTarget(target.GetEndpoint())
		if err != nil {
			return nil, err
		}
		return client.CheckHealth(ctx, new(empty.Empty), opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.Infof("CheckHealth: invoke daemon node %s CheckHealth failed: %v", target, err)
		return
	}
	return
}
