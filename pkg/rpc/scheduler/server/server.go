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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	"github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func init() {
	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		scheduler.RegisterSchedulerServer(s, &proxy{server: impl.(SchedulerServer)})
	})
}

type proxy struct {
	server SchedulerServer
	scheduler.UnimplementedSchedulerServer
}

// see scheduler.SchedulerServer
type SchedulerServer interface {
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error)
	ReportPieceResult(scheduler.Scheduler_ReportPieceResultServer) error
	ReportPeerResult(context.Context, *scheduler.PeerResult) (*base.ResponseState, error)
	LeaveTask(context.Context, *scheduler.PeerTarget) (*base.ResponseState, error)
}

func (p *proxy) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest) (rr *scheduler.RegisterResult, err error) {
	rr, err = p.server.RegisterPeerTask(ctx, ptr)

	var taskId = "unknown"
	var suc bool
	var code base.Code

	if err == nil && rr != nil {
		taskId = rr.TaskId
		if rr.State != nil {
			suc = rr.State.Success
			code = rr.State.Code
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
		zap.String("schedulerIp", dfnet.HostIp),
		zap.String("schedulerName", dfnet.HostName),
		zap.Int32("code", int32(code)))

	return
}

func (p *proxy) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	return p.server.ReportPieceResult(stream)
}

func (p *proxy) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult) (*base.ResponseState, error) {
	rs, err := p.server.ReportPeerResult(ctx, pr)

	logger.StatPeerLogger.Info("finish peer task",
		zap.Bool("success", pr.Success),
		zap.String("taskId", pr.TaskId),
		zap.String("url", pr.Url),
		zap.String("peerIp", pr.SrcIp),
		zap.String("securityDomain", pr.SecurityDomain),
		zap.String("idc", pr.Idc),
		zap.String("schedulerIp", dfnet.HostIp),
		zap.String("schedulerName", dfnet.HostName),
		zap.Int64("contentLength", pr.ContentLength),
		zap.Uint64("traffic", uint64(pr.Traffic)),
		zap.Uint32("cost", pr.Cost),
		zap.Int32("code", int32(pr.Code)))

	return rs, err
}

func (p *proxy) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget) (*base.ResponseState, error) {
	return p.server.LeaveTask(ctx, pt)
}
