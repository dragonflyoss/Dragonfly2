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
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

func init() {
	logDir := basic.HomeDir + "/logs/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/cdnsystem.log", 300, 30, 0, false, false)
	logger.SetBizLogger(bizLogger.Sugar())

	grpcLogger := logger.CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false)
	logger.SetGrpcLogger(grpcLogger.Sugar())

	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		cdnsystem.RegisterSeederServer(s, &proxy{server: impl.(SeederServer)})
	})
}

type SeederServer interface {
	// generate seeds and return to scheduler
	ObtainSeeds(context.Context, *cdnsystem.SeedRequest, chan<- *cdnsystem.PieceSeed) error
}

type proxy struct {
	server SeederServer
	cdnsystem.UnimplementedSeederServer
}

func (p *proxy) ObtainSeeds(sr *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errChan := make(chan error, 8)
	psc := make(chan *cdnsystem.PieceSeed, 4)

	once := new(sync.Once)
	closePsc := func() {
		once.Do(func() {
			close(psc)
		})
	}
	defer closePsc()

	go call(ctx, psc, p, sr, errChan)

	go send(psc, closePsc, stream, errChan)

	if err = <-errChan; dferrors.IsEndOfStream(err) {
		err = nil
	}

	return
}

func send(psc chan *cdnsystem.PieceSeed, closePsc func(), stream cdnsystem.Seeder_ObtainSeedsServer, errChan chan error) {
	err := safe.Call(func() {
		defer closePsc()

		for v := range psc {
			if err := stream.Send(v); err != nil {
				errChan <- err
				return
			}

			if v.Done {
				break
			}
		}

		errChan <- dferrors.ErrEndOfStream
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}

func call(ctx context.Context, psc chan *cdnsystem.PieceSeed, p *proxy, sr *cdnsystem.SeedRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.server.ObtainSeeds(ctx, sr, psc); err != nil {
			errChan <- rpc.ConvertServerError(err)
		}
	})

	if err != nil {
		errChan <- status.Error(codes.FailedPrecondition, err.Error())
	}
}
