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

	"d7y.io/dragonfly/v2/pkg/unit"
	"github.com/golang/protobuf/ptypes/empty"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// SchedulerServer refer to scheduler.SchedulerServer
type SchedulerServer interface {
	// RegisterPeerTask registers a peer into one task.
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error)
	// ReportPieceResult reports piece results and receives peer packets.
	ReportPieceResult(scheduler.Scheduler_ReportPieceResultServer) error
	// ReportPeerResult reports downloading result for the peer task.
	ReportPeerResult(context.Context, *scheduler.PeerResult) error
	// LeaveTask makes the peer leaving from scheduling overlay for the task.
	LeaveTask(context.Context, *scheduler.PeerTarget) error
}

type proxy struct {
	server SchedulerServer
	scheduler.UnimplementedSchedulerServer
}

func New(schedulerServer SchedulerServer, opts ...grpc.ServerOption) *grpc.Server {
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)
	scheduler.RegisterSchedulerServer(grpcServer, &proxy{server: schedulerServer})
	return grpcServer
}

func (p *proxy) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	metrics.RegisterPeerTaskCount.Inc()
	taskID := "unknown"
	isSuccess := false
	resp, err := p.server.RegisterPeerTask(ctx, req)
	if err != nil {
		taskID = resp.TaskId
		isSuccess = true
		metrics.RegisterPeerTaskFailureCount.Inc()
	}
	metrics.PeerTaskCounter.WithLabelValues(resp.SizeScope.String()).Inc()

	peerHost := req.PeerHost
	logger.StatPeerLogger.Info("Register Peer Task",
		zap.Bool("Success", isSuccess),
		zap.String("TaskID", taskID),
		zap.String("URL", req.Url),
		zap.String("PeerIP", peerHost.Ip),
		zap.String("PeerHostName", peerHost.HostName),
		zap.String("SecurityDomain", peerHost.SecurityDomain),
		zap.String("IDC", peerHost.Idc),
		zap.String("SchedulerIP", iputils.HostIP),
		zap.String("SchedulerHostName", iputils.HostName),
	)

	return resp, err
}

func (p *proxy) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	return p.server.ReportPieceResult(stream)
}

func (p *proxy) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult) (*empty.Empty, error) {
	metrics.DownloadCount.Inc()
	if req.Success {
		metrics.P2PTraffic.Add(float64(req.Traffic))
		metrics.PeerTaskDownloadDuration.Observe(float64(req.Cost))
	} else {
		metrics.DownloadFailureCount.Inc()
	}

	err := p.server.ReportPeerResult(ctx, req)

	logger.StatPeerLogger.Info("Finish Peer Task",
		zap.Bool("Success", req.Success),
		zap.String("TaskID", req.TaskId),
		zap.String("PeerID", req.PeerId),
		zap.String("URL", req.Url),
		zap.String("PeerIP", req.SrcIp),
		zap.String("SecurityDomain", req.SecurityDomain),
		zap.String("IDC", req.Idc),
		zap.String("SchedulerIP", iputils.HostIP),
		zap.String("SchedulerHostName", iputils.HostName),
		zap.String("ContentLength", unit.Bytes(req.ContentLength).String()),
		zap.String("Traffic", unit.Bytes(uint64(req.Traffic)).String()),
		zap.Duration("Cost", time.Duration(int64(req.Cost))),
		zap.Int32("Code", int32(req.Code)))

	return new(empty.Empty), err
}

func (p *proxy) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget) (*empty.Empty, error) {
	return new(empty.Empty), p.server.LeaveTask(ctx, pt)
}
