package test

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	. "github.com/onsi/ginkgo"
	"time"
)

var _ = Describe("Scheduler RPC Test", func() {
	tl := common.NewE2ELogger()
	var (
		svr    = server.NewServer()
		ss     = svr.GetServer()
		taskId string
		url  = "http://dragonfly.com/test1"
	)

	Describe("start scheduler", func(){
		It("start scheduler", func() {
			svr.GetWorker().Start()
		})
	})

	Describe("Scheduler RPC Client Test", func() {

		It("should be RegisterPeerTask successfully", func() {
			ctx := context.TODO()
			request := &scheduler.PeerTaskRequest{
				Url:    url,
				Filter: "",
				BizId:  "12345",
				UrlMata: &base.UrlMeta{
					Md5:   "",
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
				tl.Fatalf(err.Error())
			}
			if pkg == nil {
				tl.Fatalf("RegisterPeerTask pkg return nil")
				return
			}

			task, _ := mgr.GetTaskManager().GetTask(pkg.TaskId)
			if task == nil || task.TaskId != pkg.TaskId {
				tl.Fatalf("get task Failed")
				return
			}

			host, _ := mgr.GetHostManager().GetHost(request.PeerHost.Uuid)
			if host == nil || host.Uuid != request.PeerHost.Uuid {
				tl.Fatalf("get host Failed")
				return
			}

			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.Pid)
			if peerTask == nil || peerTask.Host == nil || peerTask.Host.Uuid != request.PeerHost.Uuid {
				tl.Fatalf("get peerTask Failed")
				return
			}

			peerTask = host.GetPeerTask(request.Pid)
			if peerTask == nil {
				tl.Fatalf("peerTask do not add into host")
				return
			}

			taskId = task.TaskId
		})

		It("should be do schedule once successfully", func() {
			ctx := context.TODO()
			request := &scheduler.PeerTaskRequest{
				Url:    url,
				Filter: "",
				BizId:  "12345",
				UrlMata: &base.UrlMeta{
					Md5:   "",
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
				tl.Fatalf(err.Error())
			}
			if pkg == nil {
				tl.Fatalf("RegisterPeerTask pkg return nil")
				return
			}
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(request.Pid)
			if peerTask == nil {
				tl.Fatalf("peerTask do not add into host")
				return
			}

			p := peerTask.Task.GetOrCreatePiece(0)
			p.PieceRange = "0,100"
			p.PieceMd5 = ""
			p.PieceOffset = 10
			p.PieceStyle = base.PieceStyle_PLAIN_UNSPECIFIED

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

			pieceList, _, err := scheduler.Scheduler(peerTask)
			if err != nil {
				tl.Fatalf("scheduler failed %s", err.Error())
				return
			}
			if len(pieceList) == 0 {
				tl.Fatalf("scheduler failed piece is zero")
			}
		})

		It("should be report peer result successfully", func() {
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
				tl.Fatalf(err.Error())
			}
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(result.Pid)
			if peerTask == nil || peerTask.Success != result.Success || peerTask.ErrorCode != result.ErrorCode {
				tl.Fatalf("peerTask report Failed")
				return
			}

		})

		It("should be leave task successfully", func() {
			ctx := context.TODO()
			var target = &scheduler.PeerTarget{
				TaskId: taskId,
				Pid:    "001",
			}
			resp, err := ss.LeaveTask(ctx, target)
			if err != nil {
				tl.Fatalf(err.Error())
			}
			if !resp.Success {
				tl.Fatalf("leave task Failed")
				return
			}
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(target.Pid)
			if peerTask != nil {
				tl.Fatalf("leave task Failed")
				return
			}
			host, _ := mgr.GetHostManager().GetHost("host001")
			if host == nil {
				tl.Fatalf("get host Failed")
				return
			}
			peerTask = host.GetPeerTask(target.Pid)
			if peerTask != nil {
				tl.Fatalf("peerTask do not delete from host")
				return
			}
		})
	})

	Describe("stop scheduler", func(){
		It("start scheduler", func() {
			svr.GetWorker().Stop()
		})
	})
})
