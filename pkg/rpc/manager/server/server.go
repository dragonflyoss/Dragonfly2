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

package server

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/manager"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

func init() {
	logDir := basic.HomeDir + "/logs/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/manager.log", 300, 30, 0, false, false)
	logger.SetBizLogger(bizLogger.Sugar())

	grpcLogger := logger.CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false)
	logger.SetGrpcLogger(grpcLogger.Sugar())

	gcLogger := logger.CreateLogger(logDir+"/gc.log", 300, 7, 0, false, false)
	logger.SetGcLogger(gcLogger.Sugar())

	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		manager.RegisterManagerServer(s, &proxy{server: impl.(ManagerServer)})
	})
}

type ManagerServer interface {
	GetSchedulers(context.Context, *manager.SchedulerNodeRequest) (*manager.SchedulerNodes, error)
	GetCdnNodes(context.Context, *manager.CdnNodeRequest) (*manager.CdnNodes, error)
	KeepAlive(context.Context, <-chan *manager.HeartRequest, chan<- *manager.ManagementConfig) error
}

type proxy struct {
	server ManagerServer
	manager.UnimplementedManagerServer
}

func (p *proxy) GetSchedulers(ctx context.Context, req *manager.SchedulerNodeRequest) (*manager.SchedulerNodes, error) {
	return p.server.GetSchedulers(ctx, req)
}

// get cdn server list according to client info
func (p *proxy) GetCdnNodes(ctx context.Context, req *manager.CdnNodeRequest) (*manager.CdnNodes, error) {
	return p.server.GetCdnNodes(ctx, req)
}

// keeps alive for cdn or scheduler and receives management configuration
func (p *proxy) KeepAlive(stream manager.Manager_KeepAliveServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errChan := make(chan error, 10)
	hrc := make(chan *manager.HeartRequest, 4)
	mcc := make(chan *manager.ManagementConfig, 4)
	once := new(sync.Once)
	closeMcc := func() {
		once.Do(func() {
			close(mcc)
		})
	}
	defer closeMcc()

	go receive(ctx, mcc, p, hrc, stream, errChan)

	go send(mcc, closeMcc, stream, errChan)

	if err = <-errChan; dferrors.IsEndOfStream(err) {
		err = nil
	}

	return
}

func send(mcc chan *manager.ManagementConfig, closeMcc func(), stream manager.Manager_KeepAliveServer, errChan chan error) {
	err := safe.Call(func() {
		defer closeMcc()
		for v := range mcc {
			if err := stream.Send(v); err != nil {
				errChan <- err
				return
			}
		}

		errChan <- dferrors.ErrEndOfStream
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}

func receive(ctx context.Context, mcc chan *manager.ManagementConfig, p *proxy, hrc chan *manager.HeartRequest, stream manager.Manager_KeepAliveServer, errChan chan error) {
	err := safe.Call(func() {
		go func() {
			if err := p.server.KeepAlive(ctx, hrc, mcc); err != nil {
				errChan <- rpc.ConvertServerError(err)
			}
		}()
		for {
			config, err := stream.Recv()
			if err == nil {
				hrc <- config
			} else {
				return
			}
		}
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}
