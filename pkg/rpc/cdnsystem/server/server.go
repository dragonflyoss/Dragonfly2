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
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

// SeederServer  refer to cdnsystem.SeederServer
type SeederServer interface {
	// Generate seeds and return to scheduler
	ObtainSeeds(context.Context, *cdnsystem.SeedRequest, chan<- *cdnsystem.PieceSeed) error
	// Get piece tasks from cdn
	GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)
}

type proxy struct {
	server SeederServer
	cdnsystem.UnimplementedSeederServer
}

func New(seederServer SeederServer, opts ...grpc.ServerOption) *grpc.Server {
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)
	cdnsystem.RegisterSeederServer(grpcServer, &proxy{server: seederServer})
	return grpcServer
}

func (p *proxy) ObtainSeeds(sr *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) (err error) {
	//metrics.DownloadCount.Inc()
	//metrics.ConcurrentDownloadGauge.Inc()
	//defer metrics.ConcurrentDownloadGauge.Dec()

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger obtain seed for taskID: %s, url: %s, from: %s", sr.TaskId, sr.Url, peerAddr)

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

	//if err != nil {
	//metrics.DownloadFailureCount.Inc()
	//}
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
	logger.StatSeedLogger.Info("Start Seed",
		zap.String("TaskID", taskID),
		zap.String("URL", url),
		zap.String("SeederIP", iputils.IPv4),
		zap.String("SeederHostname", hostutils.FQDNHostname))
}

func StatSeedFinish(taskID, url string, success bool, err error, startAt, finishAt time.Time, traffic, contentLength int64) {
	//metrics.DownloadTraffic.Add(float64(traffic))

	logger.StatSeedLogger.Info("Finish Seed",
		zap.Bool("Success", success),
		zap.String("TaskID", taskID),
		zap.String("URL", url),
		zap.String("SeederIP", iputils.IPv4),
		zap.String("SeederHostname", hostutils.FQDNHostname),
		zap.Time("StartAt", startAt),
		zap.Time("FinishAt", finishAt),
		zap.Int64("Traffic", traffic),
		zap.Int64("ContentLength", contentLength),
		zap.Error(err))
}
