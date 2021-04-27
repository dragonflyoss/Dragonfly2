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
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	mgClient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func GetClientByAddr(adders []dfnet.NetAddr, opts ...grpc.DialOption) (CdnClient, error) {
	if len(adders) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	cc := &cdnClient{
		rpc.NewConnection(context.Background(), "cdn-static", adders, []rpc.ConnOption{
			rpc.WithConnExpireTime(60 * time.Second),
			rpc.WithDialOption(opts),
		}),
	}
	return cc, nil
}

func GetClientByConfigServer(cfgServer mgClient.ManagerClient, cdnMap map[string]*config.CDNServerConfig, opts ...grpc.DialOption) (CdnClient, error) {
	watcher, err := newCdnListWatcher(cfgServer, 10*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cdn list watcher")
	}
	sc := &cdnClient{
		Connection: rpc.NewConnection(context.Background(), "cdn-dynamic", make([]dfnet.NetAddr, 0), []rpc.ConnOption{
			rpc.WithConnExpireTime(5 * time.Minute),
			rpc.WithDialOption(opts),
		}),
	}
	go func() {
		out := watcher.Watch()
		for adders := range out {
			var (
				dfAdders []dfnet.NetAddr
			)
			for i := range adders {
				cdnMap[adders[i].HostInfo.HostName] = &config.CDNServerConfig{
					Name:         adders[i].HostInfo.HostName,
					IP:           adders[i].HostInfo.Ip,
					RpcPort:      adders[i].RpcPort,
					DownloadPort: adders[i].DownPort,
				}
				dfAdders = append(dfAdders, dfnet.NetAddr{
					Type: dfnet.TCP,
					Addr: fmt.Sprintf("%s:%d", adders[i].HostInfo.Ip, adders[i].RpcPort),
				})
			}
			sc.Connection.UpdateState(dfAdders)
		}
	}()
	return sc, nil
}

// see cdnsystem.CdnClient
type CdnClient interface {
	ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)

	Close() error
}

type cdnClient struct {
	*rpc.Connection
}

func (cc *cdnClient) getCdnClient(key string, stick bool) (cdnsystem.SeederClient, string, error) {
	clientConn, err := cc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to get ClientConn for hashKey %s", key)
	}
	return cdnsystem.NewSeederClient(clientConn), clientConn.Target(), nil
}

func (cc *cdnClient) getSeederClientWithTarget(target string) (cdnsystem.SeederClient, error) {
	conn, err := cc.Connection.GetClientConnByTarget(target)
	if err != nil {
		return nil, err
	}
	return cdnsystem.NewSeederClient(conn), nil
}

func (cc *cdnClient) ObtainSeeds(ctx context.Context, sr *cdnsystem.SeedRequest, opts ...grpc.CallOption) (*PieceSeedStream, error) {
	return newPieceSeedStream(cc, ctx, sr.TaskId, sr, opts)
}

func (cc *cdnClient) GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, req *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		defer func() {
			logger.WithTaskID(req.TaskId).Infof("invoke cdn node %s GetPieceTasks", addr.GetEndpoint())
		}()
		if client, err := cc.getSeederClientWithTarget(addr.GetEndpoint()); err != nil {
			return nil, err
		} else {
			return client.GetPieceTasks(ctx, req, opts...)
		}
	}, 0.2, 2.0, 3, nil)
	if err == nil {
		return res.(*base.PiecePacket), nil
	}
	return nil, err
}

func init()  {
	var cc *cdnClient = nil
	var _ CdnClient = cc
}
