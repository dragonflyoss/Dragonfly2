package server

import (
	"context"
	"fmt"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/service/schedule_worker"
	"d7y.io/dragonfly/v2/scheduler/types"
)

var _ server.SchedulerServer = &SchedulerServer{}

type SchedulerServer struct {
	svc    *service.SchedulerService
	worker schedule_worker.IWorker
}

func (s *SchedulerServer) RegisterPeerTask(ctx context.Context, request *scheduler.PeerTaskRequest) (pkg *scheduler.RegisterResult, err error) {
	pkg = &scheduler.RegisterResult{}
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
			return
		}
		pkg.State = new(base.ResponseState)
		if err != nil {
			pkg.State.Code = dfcodes.SchedulerError
			pkg.State.Msg = err.Error()
			pkg.State.Success = false
			err = nil
		} else {
			pkg.State.Code = dfcodes.Success
			pkg.State.Success = true
		}
		return
	}()

	// get or create task
	pkg.TaskId = s.svc.GenerateTaskId(request.Url, request.Filter, request.UrlMata)
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
	pkg.TaskId = task.TaskId
	pkg.SizeScope = task.SizeScope

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

	switch pkg.SizeScope {
	case base.SizeScope_NORMAL:
		return
	case base.SizeScope_TINY:
		pkg.DirectPiece = task.DirectPiece
		return
	}

	// case base.SizeScope_SMALL
	// do scheduler piece
	parent, _, err := s.svc.SchedulerParent(peerTask)
	if err != nil {
		return
	}

	if parent == nil {
		pkg.SizeScope = base.SizeScope_NORMAL
		return
	}

	pkg.DirectPiece = &scheduler.RegisterResult_SinglePiece{
		SinglePiece: &scheduler.SinglePiece{
			// destination peer id
			DstPid : parent.Pid,
			// download address(ip:port)
			DstAddr: fmt.Sprintf("%s:%d", parent.Host.Ip, parent.Host.DownPort),
			// one piece task
			PieceInfo: &task.PieceList[0].PieceInfo,
		},
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
			ret.Code = dfcodes.SchedulerError
			ret.Msg = err.Error()
			ret.Success = false
			err = nil
		} else {
			ret.Code = dfcodes.Success
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

	if peerTask.Success {
		peerTask.SetNodeStatus(types.PeerTaskStatusDone)
		s.worker.ReceiveJob(peerTask)
	}

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
			ret.Code = dfcodes.SchedulerError
			ret.Msg = err.Error()
			ret.Success = false
			err = nil
		} else {
			ret.Code = dfcodes.Success
			ret.Success = true
		}
		return
	}()

	pid := target.PeerId
	peerTask, err := s.svc.GetPeerTask(pid)
	if err != nil {
		return
	}

	if peerTask != nil {
		peerTask.SetNodeStatus(types.PeerTaskStatusLeaveNode)
		s.worker.ReceiveJob(peerTask)
	}

	err = s.svc.DeletePeerTask(pid)
	return
}
