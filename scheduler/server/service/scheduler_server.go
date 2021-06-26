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
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/service/worker"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type SchedulerServer struct {
	service *service.SchedulerService
	worker  worker.IWorker
	config  config.SchedulerConfig
}

// Option is a functional option for configuring the scheduler
type Option func(p *SchedulerServer) *SchedulerServer

// WithSchedulerService sets the *service.SchedulerService
func WithSchedulerService(service *service.SchedulerService) Option {
	return func(s *SchedulerServer) *SchedulerServer {
		s.service = service

		return s
	}
}

// WithWorker sets the worker.IWorker
func WithWorker(worker worker.IWorker) Option {
	return func(p *SchedulerServer) *SchedulerServer {
		p.worker = worker

		return p
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
	resp = new(scheduler.RegisterResult)
	startTime := time.Now()
	defer func() {
		e := recover()
		if e != nil {
			err = dferrors.New(dfcodes.SchedError, fmt.Sprintf("%v", e))
			return
		}
		if err != nil {
			if _, ok := err.(*dferrors.DfError); !ok {
				err = dferrors.New(dfcodes.SchedError, err.Error())
			}
		}
		logger.Debugf("RegisterPeerTask [%s] cost time: [%d] millis", request.PeerId, time.Now().Sub(startTime).Milliseconds())
		return
	}()

	// get or create task
	//var isCdn = false
	taskID := s.service.GenerateTaskID(request.Url, request.Filter, request.UrlMeta, request.BizId, request.PeerId)
	task, ok := s.service.GetTask(taskID)
	if !ok {
		task, err = s.service.AddTask(&types.Task{
			TaskID:  resp.TaskId,
			URL:     request.Url,
			Filter:  request.Filter,
			BizID:   request.BizId,
			URLMata: request.UrlMeta,
		})
		if err != nil {
			dferror, _ := err.(*dferrors.DfError)
			if dferror != nil && dferror.Code == dfcodes.SchedNeedBackSource {
				isCdn = true
			} else {
				return
			}
		}
	}

	if task.CDNError != nil {
		err = task.CDNError
		return
	}

	// get or create host
	reqPeerHost := request.PeerHost
	if host, ok := s.service.GetHost(reqPeerHost.Uuid); !ok {
		host = &types.Host{
			Type: types.HostTypePeer,
			PeerHost: scheduler.PeerHost{
				Uuid:           reqPeerHost.Uuid,
				Ip:             reqPeerHost.Ip,
				RpcPort:        reqPeerHost.RpcPort,
				DownPort:       reqPeerHost.DownPort,
				HostName:       reqPeerHost.HostName,
				SecurityDomain: reqPeerHost.SecurityDomain,
				Location:       reqPeerHost.Location,
				Idc:            reqPeerHost.Idc,
				NetTopology:    reqPeerHost.NetTopology,
			},
		}
		//if isCdn {
		//	host.Type = types.HostTypeCdn
		//}
		host, err = s.service.AddHost(host)
		if err != nil {
			return
		}
	}

	resp.TaskId = task.TaskID
	resp.SizeScope = task.SizeScope

	// case base.SizeScope_TINY
	if resp.SizeScope == base.SizeScope_TINY {
		resp.DirectPiece = task.DirectPiece
		return
	}

	// get or creat PeerTask
	if peerTask, ok := s.service.GetPeerTask(request.PeerId); !ok {
		peerTask, err = s.service.AddPeerTask(pid, task, host)
		if err != nil
	} else if peerTask.Host == nil {
		peerTask.Host = host
	}

	if isCdn {
		peerTask.SetDown()
		err = dferrors.New(dfcodes.SchedNeedBackSource, "there is no cdn")
		return
	} else if peerTask.IsDown() {
		peerTask.SetUp()
	}

	if resp.SizeScope == base.SizeScope_NORMAL {
		return
	}

	// case base.SizeScope_SMALL
	// do scheduler piece
	parent, _, err := s.service.ScheduleParent(peerTask)
	if err != nil {
		return
	}

	if parent == nil {
		resp.SizeScope = base.SizeScope_NORMAL
		return
	}

	resp.DirectPiece = &scheduler.RegisterResult_SinglePiece{
		SinglePiece: &scheduler.SinglePiece{
			// destination peer id
			DstPid: parent.Pid,
			// download address(ip:port)
			DstAddr: fmt.Sprintf("%s:%d", parent.Host.Ip, parent.Host.DownPort),
			// one piece task
			PieceInfo: &task.PieceList[0].PieceInfo,
		},
	}

	return
}

func (s *SchedulerServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) (err error) {
	defer func() {
		e := recover()
		if e != nil {
			err = dferrors.New(dfcodes.SchedError, fmt.Sprintf("%v", e))
			return
		}
		if err != nil {
			if _, ok := err.(*dferrors.DfError); !ok {
				err = dferrors.New(dfcodes.SchedError, err.Error())
			}
		}
		return
	}()
	err = worker.NewClient(stream, s.worker, s.service).Serve()
	return
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (err error) {
	startTime := time.Now()
	defer func() {
		e := recover()
		if e != nil {
			err = dferrors.New(dfcodes.SchedError, fmt.Sprintf("%v", e))
			return
		}
		if err != nil {
			if _, ok := err.(*dferrors.DfError); !ok {
				err = dferrors.New(dfcodes.SchedError, err.Error())
			}
		}
		logger.Debugf("ReportPeerResult [%s] cost time: [%d]", result.PeerId, time.Now().Sub(startTime))
		return
	}()

	logger.Infof("[%s][%s]: receive a peer result [%+v]", result.TaskId, result.PeerId, result)

	pid := result.PeerId
	peerTask, err := s.service.GetPeerTask(pid)
	if err != nil {
		return
	}
	peerTask.SetStatus(result.Traffic, result.Cost, result.Success, result.Code)

	if peerTask.Success {
		peerTask.SetNodeStatus(types.PeerTaskStatusDone)
		s.worker.ReceiveJob(peerTask)
	} else {
		peerTask.SetNodeStatus(types.PeerTaskStatusLeaveNode)
		s.worker.ReceiveJob(peerTask)
	}

	return
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (err error) {
	startTime := time.Now()
	defer func() {
		e := recover()
		if e != nil {
			err = dferrors.New(dfcodes.SchedError, fmt.Sprintf("%v", e))
			return
		}
		if err != nil {
			if _, ok := err.(*dferrors.DfError); !ok {
				err = dferrors.New(dfcodes.SchedError, err.Error())
			}
		}
		logger.Debugf("ReportPeerResult [%s] cost time: [%d]", target.PeerId, time.Now().Sub(startTime))
		return
	}()

	pid := target.PeerId
	peerTask, err := s.service.GetPeerTask(pid)
	if err != nil {
		return
	}

	if peerTask != nil {
		peerTask.SetNodeStatus(types.PeerTaskStatusLeaveNode)
		s.worker.ReceiveJob(peerTask)
	}

	return
}
