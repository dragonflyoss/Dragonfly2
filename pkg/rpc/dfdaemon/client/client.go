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
	"errors"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func GetClient() (DaemonClient, error) {
	// 从本地文件/manager读取addrs
	return newDaemonClient(dfnet.NetAddrs{})
}

func GetClientByAddr(connType dfnet.NetworkType, addrs ...string) (DaemonClient, error) {
	// user specify
	return newDaemonClient(dfnet.NetAddrs{
		Type:  connType,
		Addrs: addrs,
	})
}

func newDaemonClient(addrs dfnet.NetAddrs, opts ...grpc.DialOption) (DaemonClient, error) {
	if len(addrs.Addrs) == 0 {
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

	CheckHealth(ctx context.Context, opts ...grpc.CallOption) (*base.ResponseState, error)
}

type daemonClient struct {
	*rpc.Connection
}

func (sc *daemonClient) getDaemonClient(key string) dfdaemon.DaemonClient{
	return dfdaemon.NewDaemonClient(sc.Connection.GetClientConn(key))
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error) {
	req.Uuid = uuid.New().String()

	drc := make(chan *dfdaemon.DownResult, 4)
	// todo 替换taskId
	drs, err := newDownResultStream(dc, ctx, req.Url, req, opts)
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

func (dc *daemonClient) CheckHealth(ctx context.Context, opts ...grpc.CallOption) (*base.ResponseState, error) {
	// todo 需要添加taskId
	return dc.getDaemonClient("").CheckHealth(ctx, &base.EmptyRequest{}, opts...)
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
