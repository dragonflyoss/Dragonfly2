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
	"sync"

	"d7y.io/dragonfly.v2/internal/dferrors"
	logger "d7y.io/dragonfly.v2/internal/dflog"
	"d7y.io/dragonfly.v2/pkg/rpc"
	"d7y.io/dragonfly.v2/pkg/rpc/base"
	"d7y.io/dragonfly.v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly.v2/pkg/safe"
	"d7y.io/dragonfly.v2/pkg/util/net/iputils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func init() {
	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		cdnsystem.RegisterSeederServer(s, &proxy{server: impl.(SeederServer)})
	})
}

// see cdnsystem.SeederServer
type SeederServer interface {
	ObtainSeeds(context.Context, *cdnsystem.SeedRequest, chan<- *cdnsystem.PieceSeed) error
	GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)
}

type proxy struct {
	server SeederServer
	cdnsystem.UnimplementedSeederServer
}

func (p *proxy) ObtainSeeds(sr *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) (err error) {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger obtain seed for taskID:%s,url:%s,from:%s", sr.TaskId, sr.Url, peerAddr)

	errChan := make(chan error, 10)
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

func (p *proxy) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return p.server.GetPieceTasks(ctx, ptr)
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
		errChan <- err
	}
}

func call(ctx context.Context, psc chan *cdnsystem.PieceSeed, p *proxy, sr *cdnsystem.SeedRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.server.ObtainSeeds(ctx, sr, psc); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- err
	}
}

func StatSeedStart(taskID, url string) {
	logger.StatSeedLogger.Info("trigger seed making",
		zap.String("taskID", taskID),
		zap.String("url", url),
		zap.String("seederIp", iputils.HostIP),
		zap.String("seederName", iputils.HostName))
}

func StatSeedFinish(taskID, url string, success bool, err error, beginTime, endTime int, traffic, contentLength int64) {
	logger.StatSeedLogger.Info("seed making finish",
		zap.Bool("success", success),
		zap.String("taskID", taskID),
		zap.String("url", url),
		zap.String("seederIp", iputils.HostIP),
		zap.String("seederName", iputils.HostName),
		zap.Int("beginTime", beginTime),
		zap.Int("endTime", endTime),
		zap.Int64("traffic", traffic),
		zap.Int64("contentLength", contentLength),
		zap.Error(err))
}
