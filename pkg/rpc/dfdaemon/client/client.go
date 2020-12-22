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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
)

import (
	"google.golang.org/grpc"
)

import (
	"context"
)

func init() {
	logDir := "/var/log/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/dfget.log", 300, -1, -1, false, false)
	log := bizLogger.Sugar()
	logger.SetBizLogger(log)
	logger.SetGrpcLogger(log)
}

type DaemonClient interface {
	// download content by dragonfly
	Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error)
	// close the conn
	Close() error
}

type daemonClient struct {
	*rpc.Connection
	Client dfdaemon.DaemonClient
}

// init client info excepting connection
var initClientFunc = func(c *rpc.Connection) {
	dc := c.Ref.(*daemonClient)
	dc.Client = dfdaemon.NewDaemonClient(c.Conn)
	dc.Connection = c
}

func CreateClient(netAddrs []basic.NetAddr) (DaemonClient, error) {
	if client, err := rpc.BuildClient(&daemonClient{}, initClientFunc, netAddrs); err != nil {
		return nil, err
	} else {
		return client.(*daemonClient), nil
	}
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemon.DownRequest, opts ...grpc.CallOption) (<-chan *dfdaemon.DownResult, error) {
	drc := make(chan *dfdaemon.DownResult, 4)

	drs, err := newDownResultStream(dc, ctx, req, opts)
	if err != nil {
		return nil, err
	}

	go receive(drs, drc)

	return drc, nil
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
				drc <- base.NewResWithErr(downResult, err).(*dfdaemon.DownResult)
				return
			}
		}
	})
}
