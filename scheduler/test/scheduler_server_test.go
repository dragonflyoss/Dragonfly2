package test

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
	"testing"
	"time"
)

var (
	svr    *server.Server
	ss     *server.SchedulerServer
	taskId string
)

func init() {
	svr = server.NewServer()
	ss = svr.GetServer()
	svr.GetWorker().Start()
}

func TestRegisterPeerTask(t *testing.T) {
	ctx := context.TODO()
	request := &scheduler.PeerTaskRequest{
		Url:    "http://www.baidu.com",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "12233",
			Range: "",
		},
		Pid: "001",
		PeerHost: &scheduler.PeerHost{
			Uuid:           "host001",
			Ip:             "127.0.0.1",
			Port:           23456,
			HostName:       "host001",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			Switch:         "",
		},
	}
	pkg, err := ss.RegisterPeerTask(ctx, request)
	if err != nil {
		t.Errorf(err.Error())
	}
	if pkg == nil {
		t.Errorf("RegisterPeerTask pkg return nil")
		return
	}

	task, _ := mgr.GetTaskManager().GetTask(pkg.TaskId)
	if task == nil || task.TaskId != pkg.TaskId {
		t.Errorf("get task Failed")
		return
	}

	host, _ := mgr.GetHostManager().GetHost(request.PeerHost.Uuid)
	if host == nil || host.Uuid != request.PeerHost.Uuid {
		t.Errorf("get host Failed")
		return
	}

	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.Pid)
	if peerTask == nil || peerTask.Host == nil || peerTask.Host.Uuid != request.PeerHost.Uuid {
		t.Errorf("get peerTask Failed")
		return
	}

	peerTask = host.GetPeerTask(request.Pid)
	if peerTask == nil {
		t.Errorf("peerTask do not add into host")
		return
	}

	taskId = task.TaskId
}

func TestScheduler(t *testing.T) {
	ctx := context.TODO()
	request := &scheduler.PeerTaskRequest{
		Url:    "http://www.baidu.com",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "12233",
			Range: "",
		},
		Pid: "002",
		PeerHost: &scheduler.PeerHost{
			Uuid:           "host002",
			Ip:             "127.0.0.1",
			Port:           22456,
			HostName:       "host002",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			Switch:         "",
		},
	}
	pkg, err := ss.RegisterPeerTask(ctx, request)
	if err != nil {
		t.Errorf(err.Error())
	}
	if pkg == nil {
		t.Errorf("RegisterPeerTask pkg return nil")
		return
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.Pid)
	if peerTask == nil {
		t.Errorf("peerTask do not add into host")
		return
	}

	peerTask.Task.AddPiece(&types.Piece{
		PieceNum:   0,
		PieceRange: "0,100",
		PieceMd5:   "11111",
		PieceOffset: 10,
		PieceStyle:  base.PieceStyle_PLAIN_UNSPECIFIED,
	})

	svr.GetWorker().ReceiveJob(&scheduler.PieceResult{
		TaskId:     taskId,
		SrcPid:     "001",
		PieceNum:   0,
		PieceRange: "0,100",
		Success:    true,
		ErrorCode:  base.Code_SUCCESS,
		Cost:       20,
	})

	time.Sleep(time.Second)

	scheduler := svr.GetSchedulerService().GetScheduler()

	pieceList, err := scheduler.Scheduler(peerTask)
	if err != nil {
		t.Errorf("scheduler failed %s", err.Error())
		return
	}
	if len(pieceList) == 0 {
		t.Errorf("scheduler failed piece is zero")
	}

}

func TestReportPeerResult(t *testing.T) {
	ctx := context.TODO()
	var result = &scheduler.PeerResult{
		TaskId:         taskId,
		Pid:            "001",
		SrcIp:          "001",
		SecurityDomain: "",
		Idc:            "",
		ContentLength:  20,
		Traffic:        20,
		Cost:           20,
		Success:        true,
		ErrorCode:      base.Code_SUCCESS,
	}
	_, err := ss.ReportPeerResult(ctx, result)
	if err != nil {
		t.Errorf(err.Error())
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(result.Pid)
	if peerTask == nil || peerTask.Success != result.Success || peerTask.ErrorCode != result.ErrorCode {
		t.Errorf("peerTask report Failed")
		return
	}
}

func TestLeaveTask(t *testing.T) {
	ctx := context.TODO()
	var target = &scheduler.PeerTarget{
		TaskId: taskId,
		Pid:    "001",
	}
	resp, err := ss.LeaveTask(ctx, target)
	if err != nil {
		t.Errorf(err.Error())
	}
	if !resp.Success {
		t.Errorf("leave task Failed")
		return
	}
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(target.Pid)
	if peerTask != nil {
		t.Errorf("leave task Failed")
		return
	}
	host, _ := mgr.GetHostManager().GetHost("host001")
	if host == nil {
		t.Errorf("get host Failed")
		return
	}
	peerTask = host.GetPeerTask(target.Pid)
	if peerTask != nil {
		t.Errorf("peerTask do not delete from host")
		return
	}
}
