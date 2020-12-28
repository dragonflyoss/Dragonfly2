package mock_client

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"math/rand"
	"sync/atomic"
	"time"
)

var clientNum = int32(0)

type MockClient struct {
	cli client.SchedulerClient
	logger  common.TestLogger
	replyChan chan *scheduler.PiecePackage_PieceTask
	in chan<- *scheduler.PieceResult
	out <-chan *scheduler.PiecePackage
	pid string
	taskId string
	waitStop chan struct{}
}

func NewMockClient(addr string, logger common.TestLogger) *MockClient {
	c, err := client.CreateClient([]basic.NetAddr{{Type: basic.TCP, Addr: addr}})
	if err != nil {
		panic(err)
	}
	pid := atomic.AddInt32(&clientNum, 1)
	mc := &MockClient{
		cli: c,
		pid: fmt.Sprintf("%04d", pid),
		logger: logger,
		replyChan : make(chan *scheduler.PiecePackage_PieceTask, 1000),
		waitStop: make(chan struct{}),
	}
	mc.Start()
	return mc
}

func (mc *MockClient) Start() {
	err := mc.registerPeerTask()
	if err != nil {
		mc.logger.Errorf("start client [%d] failed: %e", mc.pid, err)
		return
	}

	go mc.replyMessage()
}

func (mc *MockClient) GetStopChan() chan struct{} {
	return mc.waitStop
}

func (mc *MockClient) registerPeerTask() (err error) {
	request := &scheduler.PeerTaskRequest{
		Url:    "http://www.baidu.com",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		Pid: mc.pid,
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
	pkg, err := mc.cli.RegisterPeerTask(context.TODO(), request)
	if err != nil {
		mc.logger.Errorf("RegisterPeerTask failed: %e", err)
		return
	}
	if pkg == nil {
		mc.logger.Errorf("RegisterPeerTask failed: pkg is null")
		return
	}
	mc.taskId = pkg.TaskId

	wait := make(chan bool)
	go func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		mc.in, mc.out, err = mc.cli.PullPieceTasks(ctx, mc.taskId, request)
		if err != nil {
			mc.logger.Errorf("PullPieceTasks failed: %e",err)
			return
		}
		wait<-true
		mc.replyChan <- nil

		for {
			resp := <-mc.out
			if len(resp.PieceTasks)>0 {
				logMsg := fmt.Sprintf("recieve a pkg: %d, pieceNum:", len(resp.PieceTasks))
				for _, piece := range resp.PieceTasks {
					logMsg += fmt.Sprintf("[%d]", piece.PieceNum)
				}
				mc.logger.Log(logMsg)
			} else {
				mc.logger.Logf("recieve a pkg: %d\n", len(resp.PieceTasks))
			}
			if resp != nil && resp.Done {
				mc.logger.Logf("client[%d] download finished", mc.pid)
				close(mc.waitStop)
				break
			}
			if resp == nil || len(resp.PieceTasks) == 0 {
				mc.replyChan <- nil
				continue
			}
			for _, t := range resp.PieceTasks {
				mc.replyChan <- t
			}
		}

	}()
	<-wait

	return
}


func (mc *MockClient) replyMessage() {
	for t := range mc.replyChan {
		var pr *scheduler.PieceResult
		if t == nil {
			pr = &scheduler.PieceResult{
				TaskId:   mc.taskId,
				SrcPid:   mc.pid,
				PieceNum: -1,
			}
		} else {
			pr = &scheduler.PieceResult{
				TaskId:     mc.taskId,
				SrcPid:     mc.pid,
				DstPid:     t.DstPid,
				PieceNum:   t.PieceNum,
				PieceRange: t.PieceRange,
				Success:    true,
				ErrorCode:  base.Code_SUCCESS,
				Cost:       10,
			}
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(3000)))
		mc.in <- pr
	}
}

