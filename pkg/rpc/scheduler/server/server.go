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

	"github.com/golang/protobuf/ptypes/empty"

	logger "d7y.io/dragonfly.v2/internal/dflog"
	"d7y.io/dragonfly.v2/pkg/rpc"
	"d7y.io/dragonfly.v2/pkg/rpc/base"
	"d7y.io/dragonfly.v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly.v2/pkg/util/net/iputils"
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
	// RegisterPeerTask
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error)
	// ReportPieceResult
	ReportPieceResult(scheduler.Scheduler_ReportPieceResultServer) error
	// ReportPeerResult
	ReportPeerResult(context.Context, *scheduler.PeerResult) error
	// LeaveTask
	LeaveTask(context.Context, *scheduler.PeerTarget) error
}

func (p *proxy) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest) (rr *scheduler.RegisterResult, err error) {
	rr, err = p.server.RegisterPeerTask(ctx, ptr)

	var taskID = "unknown"
	var suc bool
	var code base.Code

	if err == nil && rr != nil {
		taskID = rr.TaskId
	}

	peerHost := ptr.PeerHost

	logger.StatPeerLogger.Info("register peer task",
		zap.Bool("success", suc),
		zap.String("taskID", taskID),
		zap.String("url", ptr.Url),
		zap.String("peerIp", peerHost.Ip),
		zap.String("securityDomain", peerHost.SecurityDomain),
		zap.String("idc", peerHost.Idc),
		zap.String("schedulerIp", iputils.HostIP),
		zap.String("schedulerName", iputils.HostName),
		zap.Int32("code", int32(code)))

	return
}

func (p *proxy) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	return p.server.ReportPieceResult(stream)
}

func (p *proxy) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult) (*empty.Empty, error) {
	err := p.server.ReportPeerResult(ctx, pr)

	logger.StatPeerLogger.Info("finish peer task",
		zap.Bool("success", pr.Success),
		zap.String("taskID", pr.TaskId),
		zap.String("url", pr.Url),
		zap.String("peerIp", pr.SrcIp),
		zap.String("securityDomain", pr.SecurityDomain),
		zap.String("idc", pr.Idc),
		zap.String("schedulerIp", iputils.HostIP),
		zap.String("schedulerName", iputils.HostName),
		zap.Int64("contentLength", pr.ContentLength),
		zap.Uint64("traffic", uint64(pr.Traffic)),
		zap.Uint32("cost", pr.Cost),
		zap.Int32("code", int32(pr.Code)))

	return new(empty.Empty), err
}

func (p *proxy) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget) (*empty.Empty, error) {
	return new(empty.Empty), p.server.LeaveTask(ctx, pt)
}
