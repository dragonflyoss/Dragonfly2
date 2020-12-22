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
	dferror "github.com/dragonflyoss/Dragonfly2/pkg/error"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

func init() {
	logDir := "/var/log/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/daemon.log", 100, 7, 15, false, false)
	log := bizLogger.Sugar()
	logger.SetBizLogger(log)
	logger.SetGrpcLogger(log)

	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		dfdaemon.RegisterDaemonServer(s, &proxy{server: impl.(DaemonServer)})
	})
}

// DaemonServer is the server API for Daemon service.
type DaemonServer interface {
	// download content by dragonfly
	Download(context.Context, *dfdaemon.DownRequest, chan<- *dfdaemon.DownResult) error
}

type proxy struct {
	server DaemonServer
	dfdaemon.UnimplementedDaemonServer
}

func (p *proxy) Download(req *dfdaemon.DownRequest, stream dfdaemon.Daemon_DownloadServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errChan := make(chan error, 8)
	drc := make(chan *dfdaemon.DownResult, 4)

	once := new(sync.Once)
	closeDrc := func() {
		once.Do(func() {
			close(drc)
		})
	}
	defer closeDrc()

	go call(ctx, drc, p, req, errChan)

	go send(drc, closeDrc, stream, errChan)

	if err = <-errChan; err == dferror.EOS {
		err = nil
	}

	return
}

func send(drc chan *dfdaemon.DownResult, closeDrc func(), stream dfdaemon.Daemon_DownloadServer, errChan chan error) {
	err := safe.Call(func() {
		defer closeDrc()

		for v := range drc {
			if err := stream.Send(v); err != nil {
				errChan <- err
				return
			}

			if v.Done {
				break
			}
		}

		errChan <- dferror.EOS
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}

func call(ctx context.Context, drc chan *dfdaemon.DownResult, p *proxy, req *dfdaemon.DownRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.server.Download(ctx, req, drc); err != nil {
			errChan <- rpc.ConvertServerError(err)
		}
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}
