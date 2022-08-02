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

//go:generate mockgen -destination mocks/client_mock.go -source client.go -package mocks

package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/pkg/apis/dfdaemon/v1"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

var _ DaemonClient = (*daemonClient)(nil)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (DaemonClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of daemon is empty")
	}

	dialOpts, err := rpc.VsockDialerOption(addrs, opts)
	if err != nil {
		return nil, err
	}
	dc := &daemonClient{
		rpc.NewConnection(context.Background(), "daemon-static", addrs, []rpc.ConnOption{
			rpc.WithConnExpireTime(60 * time.Second),
			rpc.WithDialOption(dialOpts),
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
				rpc.WithConnExpireTime(60 * time.Second),
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

// DaemonClient see dfdaemonv1.DaemonClient
type DaemonClient interface {
	Download(ctx context.Context, req *dfdaemonv1.DownRequest, opts ...grpc.CallOption) (*DownResultStream, error)

	GetPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (*commonv1.PiecePacket, error)

	SyncPieceTasks(ctx context.Context, addr dfnet.NetAddr, ptr *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_SyncPieceTasksClient, error)

	CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) error

	StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest, opts ...grpc.CallOption) error

	ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest, opts ...grpc.CallOption) error

	ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, opts ...grpc.CallOption) error

	DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest, opts ...grpc.CallOption) error

	Close() error
}

type daemonClient struct {
	*rpc.Connection
}

func (dc *daemonClient) getDaemonClient(key string, stick bool) (dfdaemonv1.DaemonClient, string, error) {
	clientConn, err := dc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", err
	}
	return dfdaemonv1.NewDaemonClient(clientConn), clientConn.Target(), nil
}

func (dc *daemonClient) getDaemonClientWithTarget(target string) (dfdaemonv1.DaemonClient, error) {
	conn, err := dc.Connection.GetClientConnByTarget(target)
	if err != nil {
		return nil, err
	}
	return dfdaemonv1.NewDaemonClient(conn), nil
}

func (dc *daemonClient) Download(ctx context.Context, req *dfdaemonv1.DownRequest, opts ...grpc.CallOption) (*DownResultStream, error) {
	req.Uuid = uuid.New().String()
	// generate taskID
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	return newDownResultStream(ctx, dc, taskID, req, opts)
}

func (dc *daemonClient) GetPieceTasks(ctx context.Context, target dfnet.NetAddr, ptr *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (*commonv1.PiecePacket,
	error) {
	client, err := dc.getDaemonClientWithTarget(target.GetEndpoint())
	if err != nil {
		return nil, err
	}
	return client.GetPieceTasks(ctx, ptr, opts...)
}

func (dc *daemonClient) SyncPieceTasks(ctx context.Context, target dfnet.NetAddr, ptr *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_SyncPieceTasksClient, error) {
	client, err := dc.getDaemonClientWithTarget(target.GetEndpoint())
	if err != nil {
		return nil, err
	}
	syncClient, err := client.SyncPieceTasks(ctx, opts...)
	if err != nil {
		logger.WithTaskID(ptr.TaskId).Infof("SyncPieceTasks: invoke daemon node %s SyncPieceTasks failed: %v", target, err)
		return nil, err
	}

	return syncClient, syncClient.Send(ptr)
}

func (dc *daemonClient) CheckHealth(ctx context.Context, target dfnet.NetAddr, opts ...grpc.CallOption) (err error) {
	_, err = rpc.ExecuteWithRetry(func() (any, error) {
		client, err := dc.getDaemonClientWithTarget(target.GetEndpoint())
		if err != nil {
			return nil, fmt.Errorf("failed to connect server %s: %v", target.GetEndpoint(), err)
		}
		return client.CheckHealth(ctx, new(emptypb.Empty), opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.Infof("CheckHealth: invoke daemon node %s CheckHealth failed: %v", target, err)
		return
	}
	return
}

func (dc *daemonClient) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest, opts ...grpc.CallOption) error {
	// StatTask is a latency sensitive operation, so we don't retry & wait for daemon to start,
	// we assume daemon is already running.
	taskID := idgen.TaskID(req.Url, req.UrlMeta)

	client, _, err := dc.getDaemonClient(taskID, false)
	if err != nil {
		return err
	}
	_, err = client.StatTask(ctx, req, opts...)
	return err
}

func (dc *daemonClient) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest, opts ...grpc.CallOption) error {
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	client, _, err := dc.getDaemonClient(taskID, false)
	if err != nil {
		return err
	}
	_, err = client.ImportTask(ctx, req, opts...)
	return err
}

func (dc *daemonClient) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, opts ...grpc.CallOption) error {
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	client, _, err := dc.getDaemonClient(taskID, false)
	if err != nil {
		return err
	}
	_, err = client.ExportTask(ctx, req, opts...)
	return err
}

func (dc *daemonClient) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest, opts ...grpc.CallOption) error {
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	client, _, err := dc.getDaemonClient(taskID, false)
	if err != nil {
		return err
	}
	_, err = client.DeleteTask(ctx, req, opts...)
	return err
}
