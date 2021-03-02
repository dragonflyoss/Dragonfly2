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
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/pkg/util/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func GetClient() (DaemonClient, error) {
	// 从本地文件/manager读取addrs
	return newDaemonClient([]dfnet.NetAddr{})
}

func GetClientByAddr(addrs []dfnet.NetAddr) (DaemonClient, error) {
	// user specify
	return newDaemonClient(addrs)
}

func newDaemonClient(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	return &daemonClient{
		rpc.NewConnection(addrs, opts...),
	}, nil
}

// see dfdaemon.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error)

	GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (*base.ResponseState, error)
}

type daemonClient struct {
	*rpc.Connection
}

func (dc *daemonClient) getDaemonClient(key string) dfdaemon.DaemonClient {
	return dfdaemon.NewDaemonClient(dc.Connection.GetClientConn(key))
}

func (dc *daemonClient) getDaemonClientWithTarget(target string) (dfdaemon.DaemonClient, error) {
	conn, err := dc.Connection.GetClientConnByTarget(target)
	if err != nil {
		return nil, err
	}
	return dfdaemon.NewDaemonClient(conn), nil
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error) {
	req.Uuid = uuid.New().String()

	drc := make(chan *dfdaemon.DownResult, 4)
	// 生成taskId
	taskId := idutils.GenerateTaskId(req.Url, req.Filter, req.UrlMeta)
	drs, err := newDownResultStream(dc, ctx, taskId, req, opts)
	if err != nil {
		return nil, err
	}

	go receive(drs, drc)

	return drc, nil
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return dc.getDaemonClient(ptr.TaskId).GetPieceTasks(ctx, ptr, opts...)
	}, 0.2, 2.0, 3, nil)

	if err == nil {
		return res.(*base.PiecePacket), nil
	}

	return nil, err
}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (*base.ResponseState, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := dc.getDaemonClientWithTarget(target.GetEndpoint()); err != nil {
			return nil, errors.Wrapf(err, "failed to connect server %s", target.GetEndpoint())
		} else {
			return client.CheckHealth(ctx, &base.EmptyRequest{}, opts...)
		}
	}, 0.2, 2.0, 3, nil)
	if err == nil {
		return res.(*base.ResponseState), nil
	}
	return nil, err
}

func receive(drs *downResultStream, drc chan *dfdaemon.DownResult) {
	safe.Call(func() {
		defer close(drc)

		for {
			downResult, err := drs.recv()
			if err == nil {
				drc <- downResult
				if downResult.Done {
					return
				}
			} else {
				drc <- common.NewResWithErr(downResult, err).(*dfdaemon.DownResult)
				return
			}
		}
	})
}
