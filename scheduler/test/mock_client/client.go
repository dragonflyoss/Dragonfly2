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
	cli           client.SchedulerClient
	logger        common.TestLogger
	replyChan     chan *scheduler.PiecePackage_PieceTask
	replyFinished chan struct{}
	in            chan<- *scheduler.PieceResult
	out           <-chan *scheduler.PiecePackage
	pid           string
	taskId        string
	waitStop      chan struct{}
}

func NewMockClient(addr string, logger common.TestLogger) *MockClient {
	c, err := client.CreateClient([]basic.NetAddr{{Type: basic.TCP, Addr: addr}})
	if err != nil {
		panic(err)
	}
	pid := atomic.AddInt32(&clientNum, 1)
	mc := &MockClient{
		cli:           c,
		pid:           fmt.Sprintf("%04d", pid),
		logger:        logger,
		replyChan:     make(chan *scheduler.PiecePackage_PieceTask, 1000),
		waitStop:      make(chan struct{}),
		replyFinished: make(chan struct{}),
	}
	logger.Logf("NewMockClient %s", mc.pid)
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
		Url:    "http://dragonfly.com/test-multi",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		Pid: mc.pid,
		PeerHost: &scheduler.PeerHost{
			Uuid:           fmt.Sprintf("host%s", mc.pid),
			Ip:             "127.0.0.1",
			Port:           23456,
			HostName:       fmt.Sprintf("host%s", mc.pid),
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			Switch:         "",
		},
	}
	pkg, err := mc.cli.RegisterPeerTask(context.TODO(), request)
	if err != nil {
		mc.logger.Errorf("[%s] RegisterPeerTask failed: %e", mc.pid, err)
		return
	}
	if pkg == nil {
		mc.logger.Errorf("[%s] RegisterPeerTask failed: pkg is null", mc.pid)
		return
	}
	mc.taskId = pkg.TaskId

	for _, t := range pkg.PieceTasks {
		mc.replyChan <- t
	}

	wait := make(chan bool)
	go func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		mc.in, mc.out, err = mc.cli.PullPieceTasks(ctx, mc.taskId, request)
		if err != nil {
			mc.logger.Errorf("[%s] PullPieceTasks failed: %e", mc.pid, err)
			return
		}
		wait <- true
		mc.replyChan <- nil

		for {
			resp := <-mc.out
			if len(resp.PieceTasks) > 0 {
				logMsg := fmt.Sprintf("[%s] recieve a pkg: %d, pieceNum:", mc.pid, len(resp.PieceTasks))
				for _, piece := range resp.PieceTasks {
					logMsg += fmt.Sprintf("[%d-%s]", piece.PieceNum, piece.DstPid)
				}
				mc.logger.Log(logMsg)
			} else {
				mc.logger.Logf("[%s] receive a pkg: pieceNum[%d]\n", mc.pid, len(resp.PieceTasks))
			}
			if resp != nil && resp.Done {
				mc.logger.Logf("client[%s] download finished", mc.pid)
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
		<-mc.replyFinished
	}()
	<-wait

	return
}

func (mc *MockClient) replyMessage() {
	defer func() {
		recover()
		close(mc.replyFinished)
	}()
	closed := false
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
		select {
		case _, notClosed := <-mc.waitStop:
			if !notClosed {
				if !closed {
					close(mc.replyChan)
				}
			}
		default:
		}
		mc.in <- pr
	}
}
