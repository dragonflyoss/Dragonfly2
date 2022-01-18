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
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/cdn/metrics"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

// SeederServer  refer to cdnsystem.SeederServer
type SeederServer interface {
	// ObtainSeeds generate seeds and return to scheduler
	ObtainSeeds(req *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) error
	// GetPieceTasks get piece tasks from cdn
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
	metrics.DownloadCount.Inc()
	metrics.ConcurrentDownloadGauge.Inc()
	defer metrics.ConcurrentDownloadGauge.Dec()

	err = p.server.ObtainSeeds(sr, stream)

	if err != nil {
		metrics.DownloadFailureCount.Inc()
	}
	return
}

func (p *proxy) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return p.server.GetPieceTasks(ctx, ptr)
}

func StatSeedStart(taskID, url string) {
	logger.StatSeedLogger.Info("Start Seed",
		zap.String("TaskID", taskID),
		zap.String("URL", url),
		zap.String("SeederIP", iputils.IPv4),
		zap.String("SeederHostname", hostutils.FQDNHostname))
}

func StatSeedFinish(taskID, url string, success bool, err error, startAt, finishAt time.Time, traffic, contentLength int64) {
	metrics.DownloadTraffic.Add(float64(traffic))

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
