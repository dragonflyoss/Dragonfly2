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

package service

import (
	"context"
	"io"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base/common"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	ants "github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
)

type SchedulerServer struct {
	service     *core.SchedulerService
	worker      *ants.Pool
	config      config.SchedulerConfig
	peerManager daemon.PeerMgr
	taskManager daemon.TaskMgr
}

// Option is a functional option for configuring the scheduler
type Option func(p *SchedulerServer) *SchedulerServer

// WithSchedulerService sets the *service.SchedulerService
func WithSchedulerService(service *core.SchedulerService) Option {
	return func(s *SchedulerServer) *SchedulerServer {
		s.service = service

		return s
	}
}

// NewSchedulerServer returns a new transparent scheduler server from the given options
func NewSchedulerServer(cfg *config.Config, options ...Option) *SchedulerServer {
	return NewSchedulerWithOptions(cfg, options...)
}

// NewSchedulerWithOptions constructs a new instance of a scheduler server with additional options.
func NewSchedulerWithOptions(cfg *config.Config, options ...Option) *SchedulerServer {
	scheduler := &SchedulerServer{
		config: cfg.Scheduler,
	}

	for _, opt := range options {
		opt(scheduler)
	}

	return scheduler
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
	if err := validateParams(request); err != nil {
		return nil, dferrors.Newf(dfcodes.BadRequest, "bad request param for register peer task: %v", err)
	}
	taskID := s.service.GenerateTaskID(request.Url, request.Filter, request.UrlMeta, request.BizId, request.PeerId)
	task, ok := s.taskManager.Get(taskID)
	if !ok {
		if task, err = s.service.AddTask(types.NewTask(taskID, request.Url, request.Filter, request.BizId, request.UrlMeta)); err != nil {
			return nil
		}
	}
	resp, err := s.service.RegisterPeerTask(types.NewTask(taskID, request.Url, request.Filter, request.BizId, request.UrlMeta))

	return
}

func (s *SchedulerServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	for {
		select {
		case <-stream.Context().Done():
			logger.Infof()
		default:
			pieceResult, err := stream.Recv()
			if err == io.EOF || pieceResult.PieceNum == common.EndOfPiece {
				logger.Infof("read all piece result")
				return nil
			}
			if err != nil {
				// 处理piece error
				return err
			}
		}
	}
	//err = worker.NewClient(stream, s.worker, s.service).Serve()
	//return
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	peerTask, ok := s.service.GetPeerTask(result.PeerId)
	if !ok {
		logger.Warnf("")
		return errors.New()
	}
	peerTask.Traffic = result.Traffic
	peerTask.Cost = time.Duration(result.Cost)
	peerTask.Success = result.Success
	peerTask.Code = result.Code
	peerTask.SetStatus(result.Traffic, result.Cost, result.Success, result.Code)

	if peerTask.Success {
		peerTask.Status = types.PeerStatusDone
		s.worker.Submit(peerTask)
	} else {
		peerTask.Status = types.PeerStatusLeaveNode
		s.worker.Submit(peerTask)
	}

	return
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	peerNode, ok := s.service.GetPeerTask(target.PeerId)
	if !ok {
		logger.Warnf("leave task: peer %d not exists", target.PeerId)
		return nil
	}
	peerNode.Status = types.PeerStatusLeaveNode
	return s.worker.Submit(peerNode)
}

// validateParams validates the params of scheduler.PeerTaskRequest.
func validateParams(req *scheduler.PeerTaskRequest) error {
	if !urlutils.IsValidURL(req.Url) {
		return errors.Wrapf(errortypes.ErrInvalidValue, "raw url: %s", req.Url)
	}

	if stringutils.IsEmpty(req.PeerId) {
		return errors.Wrapf(errortypes.ErrEmptyValue, "path")
	}
	return nil
}
