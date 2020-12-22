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
	"github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func init() {
	logDir := basic.HomeDir + "/logs/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/scheduler.log", 300, 30, 0, false, false)
	logger.SetBizLogger(bizLogger.Sugar())

	grpcLogger := logger.CreateLogger(logDir+"/grpc.log", 300, 30, 0, false, false)
	logger.SetGrpcLogger(grpcLogger.Sugar())

	statPeerLogger := logger.CreateLogger(logDir+"/stat/peer.log", 300, 30, 0, true, true)
	logger.SetStatPeerLogger(statPeerLogger)

	statSeedLogger := logger.CreateLogger(logDir+"/stat/seed.log", 300, 30, 0, true, true)
	logger.SetStatSeedLogger(statSeedLogger)

	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		scheduler.RegisterSchedulerServer(s, &proxy{server: impl.(SchedulerServer)})
	})
}

type SchedulerServer interface {
	// RegisterPeerTask registers a peer into one task and returns a piece package immediately
	// if task resource is enough.
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest) (*scheduler.PiecePackage, error)
	// PullPieceTasks get piece results and return piece tasks.
	// PieceResult chan is used to get stream request and PiecePackage chan is used to return stream response.
	// Closed PieceResult chan indicates that request stream reaches end.
	// Closed PiecePackage chan indicates that response stream reaches end.
	//
	// For PiecePackage chan, send func must bind a recover, it is recommended that using safe.Call wraps
	// these func.
	//
	// On error, it will abort the stream.
	PullPieceTasks(context.Context, scheduler.Scheduler_PullPieceTasksServer) error
	// ReportPeerResult reports downloading result for one peer task.
	ReportPeerResult(context.Context, *scheduler.PeerResult) (*base.ResponseState, error)
	// LeaveTask makes the peer leaving from the task scheduling overlay.
	LeaveTask(context.Context, *scheduler.PeerTarget) (*base.ResponseState, error)
}

type proxy struct {
	server SchedulerServer
	scheduler.UnimplementedSchedulerServer
}

func (p *proxy) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest) (pp *scheduler.PiecePackage, err error) {
	pp, err = p.server.RegisterPeerTask(ctx, ptr)
	err = rpc.ConvertServerError(err)

	var taskId = "unknown"
	var suc bool
	var code base.Code

	if err == nil && pp != nil {
		taskId = pp.TaskId
		if pp.State != nil {
			suc = pp.State.Success
			code = pp.State.Code
		}
	}

	peerHost := ptr.PeerHost

	logger.StatPeerLogger.Info("register peer task",
		zap.Bool("success", suc),
		zap.String("taskId", taskId),
		zap.String("url", ptr.Url),
		zap.String("peerIp", peerHost.Ip),
		zap.String("securityDomain", peerHost.SecurityDomain),
		zap.String("idc", peerHost.Idc),
		zap.String("schedulerIp", basic.LocalIp),
		zap.Int("code", int(code)))

	return
}

func (p *proxy) PullPieceTasks(stream scheduler.Scheduler_PullPieceTasksServer) (err error) {
	ctx, _ := context.WithCancel(stream.Context())
	return p.server.PullPieceTasks(ctx, stream)
}

// The peer's result is determined by itself but not scheduler.
func (p *proxy) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult) (*base.ResponseState, error) {
	logger.StatPeerLogger.Info("finish peer task",
		zap.Bool("success", pr.Success),
		zap.String("taskId", pr.TaskId),
		zap.String("url", pr.Url),
		zap.String("peerIp", pr.SrcIp),
		zap.String("securityDomain", pr.SecurityDomain),
		zap.String("idc", pr.Idc),
		zap.String("schedulerIp", basic.LocalIp),
		zap.Int64("contentLength", pr.ContentLength),
		zap.Uint64("traffic", pr.Traffic),
		zap.Uint32("cost", pr.Cost),
		zap.Int("code", int(pr.ErrorCode)))

	rs, err := p.server.ReportPeerResult(ctx, pr)

	return rs, rpc.ConvertServerError(err)
}

func (p *proxy) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget) (*base.ResponseState, error) {
	rs, err := p.server.LeaveTask(ctx, pt)
	return rs, rpc.ConvertServerError(err)
}
