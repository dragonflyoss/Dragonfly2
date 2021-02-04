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
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// see dfdaemon.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error)
	GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)
	CheckHealth(ctx context.Context, opts ...grpc.CallOption) (*base.ResponseState, error)
	Close() error
}

type daemonClient struct {
	*rpc.Connection
	Client dfdaemon.DaemonClient
}

var initClientFunc = func(c *rpc.Connection) {
	dc := c.Ref.(*daemonClient)
	dc.Client = dfdaemon.NewDaemonClient(c.Conn)
	dc.Connection = c
}

func CreateClient(netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	if client, err := rpc.BuildClient(&daemonClient{}, initClientFunc, netAddrs, opts); err != nil {
		return nil, err
	} else {
		return client.(*daemonClient), nil
	}
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error) {
	req.Uuid = uuid.New().String()

	drc := make(chan *dfdaemon.DownResult, 4)

	drs, err := newDownResultStream(dc, ctx, req, opts)
	if err != nil {
		return nil, err
	}

	go receive(drs, drc)

	return drc, nil
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return dc.Client.GetPieceTasks(ctx, ptr, opts...)
	}, 0.2, 2.0, 3, nil)

	if err == nil {
		return res.(*base.PiecePacket), nil
	}

	return nil, err
}

func (dc *daemonClient) CheckHealth(ctx context.Context, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return dc.Client.CheckHealth(ctx, &base.EmptyRequest{}, opts...)
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
