package server

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service"
	"github.com/dragonflyoss/Dragonfly2/scheduler/service/schedule_worker"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

var _ server.SchedulerServer = &SchedulerServer{}

type SchedulerServer struct {
	svc    *service.SchedulerService
	worker schedule_worker.IWorker
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (pkg *scheduler.PeerPacket, err error) {
	pkg = &scheduler.PeerPacket{SrcPid: request.PeerId}
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
			return
		}
		pkg.State = new(base.ResponseState)
		if err != nil {
			pkg.State.Code = base.Code_SCHEDULER_ERROR
			pkg.State.Msg = err.Error()
			pkg.State.Success = false
			err = nil
		} else {
			pkg.State.Code = base.Code_SUCCESS
			pkg.State.Success = true
		}
		return
	}()

	// get or create task
	pkg.TaskId = s.svc.GenerateTaskId(request.Url, request.Filter)
	task, _ := s.svc.GetTask(pkg.TaskId)
	if task == nil {
		task = &types.Task{
			TaskId:  pkg.TaskId,
			Url:     request.Url,
			Filter:  request.Filter,
			BizId:   request.BizId,
			UrlMata: request.UrlMata,
		}
		task, err = s.svc.AddTask(task)
		if err != nil {
			return
		}
	}

	// get or create host
	hostId := request.PeerHost.Uuid
	host, _ := s.svc.GetHost(hostId)
	if host == nil {
		host = &types.Host{
			Type:           types.HostTypePeer,
			PeerHost: *request.PeerHost,
		}
		host, err = s.svc.AddHost(host)
		if err != nil {
			return
		}
	}

	// get or creat PeerTask
	pid := request.PeerId
	peerTask, _ := s.svc.GetPeerTask(pid)
	if peerTask == nil {
		peerTask, err = s.svc.AddPeerTask(pid, task, host)
		if err != nil {
			return
		}
	} else if peerTask.Host == nil {
		peerTask.Host = host
	}
	if peerTask.IsDown() {
		peerTask.SetUp()
	}

	// do scheduler piece
	parent, _, err := s.svc.SchedulerParent(peerTask)
	if err != nil {
		return
	}

	// assemble result
	if parent != nil {
		pkg = peerTask.GetSendPkg()
	}

	return
}

func (s *SchedulerServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	return schedule_worker.CreateClient(stream, s.worker, s.svc.GetScheduler()).Start()
}

func (s *SchedulerServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (ret *base.ResponseState, err error) {
	ret = new(base.ResponseState)
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
			return
		}
		if err != nil {
			ret.Code = base.Code_SCHEDULER_ERROR
			ret.Msg = err.Error()
			ret.Success = false
			err = nil
		} else {
			ret.Code = base.Code_SUCCESS
			ret.Success = true
		}
		return
	}()

	pid := result.SrcIp
	peerTask, err := s.svc.GetPeerTask(pid)
	if err != nil {
		return
	}
	peerTask.SetStatus(result.Traffic, result.Cost, result.Success, result.Code)

	return
}

func (s *SchedulerServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (ret *base.ResponseState, err error) {
	ret = new(base.ResponseState)
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
			return
		}
		if err != nil {
			ret.Code = base.Code_SCHEDULER_ERROR
			ret.Msg = err.Error()
			ret.Success = false
			err = nil
		} else {
			ret.Code = base.Code_SUCCESS
			ret.Success = true
		}
		return
	}()

	pid := target.PeerId

	err = s.svc.DeletePeerTask(pid)

	return
}
