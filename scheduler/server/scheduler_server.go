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

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (pkg *scheduler.PiecePackage, err error) {
	pkg = new(scheduler.PiecePackage)
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
			Uuid:           request.PeerHost.Uuid,
			Ip:             request.PeerHost.Ip,
			Port:           request.PeerHost.Port,
			HostName:       request.PeerHost.HostName,
			SecurityDomain: request.PeerHost.SecurityDomain,
			Location:       request.PeerHost.Location,
			Idc:            request.PeerHost.Idc,
			Switch:         request.PeerHost.Switch,
		}
		host, err = s.svc.AddHost(host)
		if err != nil {
			return
		}
	}

	// get or creat PeerTask
	pid := request.Pid
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
	_ = parent

	return
}

func (s *SchedulerServer) PullPieceTasks(server scheduler.Scheduler_PullPieceTasksServer) (err error) {
	schedule_worker.CreateClient(server, s.worker, s.svc.GetScheduler()).Start()
	return
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
	peerTask.SetStatus(result.Traffic, result.Cost, result.Success, result.ErrorCode)

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

	pid := target.Pid

	err = s.svc.DeletePeerTask(pid)

	return
}
