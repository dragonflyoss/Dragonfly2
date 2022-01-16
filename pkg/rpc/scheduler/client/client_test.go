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

package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ scheduler.SchedulerServer = (*testServer)(nil)

type testServer struct {
	scheduler.UnimplementedSchedulerServer

	registerResult map[string]*scheduler.RegisterResult // url->registerResult
	taskInfo       map[string][]*scheduler.PeerPacket   // taskID-> peerPacket
	taskUrl2Id     map[string]string                    //url->taskID
	taskID2Url     map[string]string
	addr           string
}

// RegisterPeerTask registers a peer into one task.
func (s *testServer) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	if rs, ok := s.registerResult[req.Url]; !ok {
		return nil, status.Error(codes.NotFound, "wrong server")
	} else {
		s.taskUrl2Id[req.Url] = idgen.TaskID(req.Url, nil)
		s.taskID2Url[idgen.TaskID(req.Url, nil)] = req.Url
		return rs, nil
	}
}

// ReportPieceResult reports piece results and receives peer packets.
// when migrating to another scheduler,
// it will send the last piece result to the new scheduler.
func (s *testServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		_, ok := s.taskID2Url[in.TaskId]
		if !ok {
			return status.Error(codes.NotFound, "task not found")
		}
		if err := stream.Send(&scheduler.PeerPacket{}); err != nil {
			return err
		}
	}
	return nil
}

// ReportPeerResult reports downloading result for the peer task.
func (s *testServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (*emptypb.Empty, error) {
	if _, ok := s.taskUrl2Id[result.TaskId]; !ok {
		return nil, status.Error(codes.NotFound, "wrong server")
	}
	return &emptypb.Empty{}, nil
}

// LeaveTask makes the peer leaving from scheduling overlay for the task.
func (s *testServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (*emptypb.Empty, error) {
	if _, ok := s.taskInfo[target.TaskId]; !ok {
		return nil, status.Error(codes.NotFound, "wrong server")
	}
	return &emptypb.Empty{}, nil
}

func newTestServer(addr string, registerResult map[string]*scheduler.RegisterResult) *testServer {
	s := &testServer{
		registerResult: registerResult,
		taskInfo:       make(map[string][]*scheduler.PeerPacket),
		taskUrl2Id:     make(map[string]string),
		taskID2Url:     make(map[string]string),
		addr:           addr,
	}
	return s
}

type testServerData struct {
	servers     []*grpc.Server
	serverImpls []*testServer
	addresses   []string
}

func (t *testServerData) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestServers(count int) (_ *testServerData, err error) {
	t := &testServerData{}
	registerResult, _, _ := loadTestData()
	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	var taskUrls []string
	for s, _ := range registerResult {
		taskUrls = append(taskUrls, s)
	}
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		var rs map[string]*scheduler.RegisterResult
		if count != 1 {
			rs = map[string]*scheduler.RegisterResult{
				taskUrls[i%len(taskUrls)]: registerResult[taskUrls[i%len(taskUrls)]],
			}
		} else {
			rs = registerResult
		}
		sImpl := newTestServer(lis.Addr().String(), rs)
		scheduler.RegisterSchedulerServer(s, sImpl)
		t.servers = append(t.servers, s)
		t.serverImpls = append(t.serverImpls, sImpl)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return t, nil
}

func TestOneBackend(t *testing.T) {
	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	client, err := GetClientByAddrs([]dfnet.NetAddr{{Addr: test.addresses[0]}})
	if err != nil {
		t.Fatalf("failed to get daemon client: %v", err)
	}
	defer client.Close()

	var testURL = "https://www.dragonfly1.com"
	{
		taskRequest := &scheduler.PeerTaskRequest{
			Url:         testURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}
		result, err := client.RegisterPeerTask(context.Background(), taskRequest)
		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		if !cmp.Equal(result, test.serverImpls[0].registerResult[testURL], cmpopts.IgnoreUnexported(scheduler.RegisterResult{}),
			cmpopts.IgnoreUnexported(scheduler.SinglePiece{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
			t.Fatalf("registerResult is not same as expected, expected: %s, actual %s", test.serverImpls[0].registerResult[testURL], result)
		}
	}
	{
		taskRequest := &scheduler.PeerTaskRequest{
			Url:         testURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}
		stream, err := client.ReportPieceResult(context.Background(), "", taskRequest)
		if err != nil {
			t.Fatalf("failed to call ReportPieceResult: %v", err)
		}
		for {
			stream.Recv()
		}
	}
	{
		peerResult := &scheduler.PeerResult{
			TaskId:          "",
			PeerId:          "",
			SrcIp:           "",
			SecurityDomain:  "",
			Idc:             "",
			Url:             "",
			ContentLength:   0,
			Traffic:         0,
			Cost:            0,
			Success:         false,
			Code:            0,
			TotalPieceCount: 0,
		}
		client.ReportPeerResult(context.Background(), peerResult)
	}
	{
		client.LeaveTask(context.Background(), &scheduler.PeerTarget{
			TaskId: "",
			PeerId: "",
		})
	}
}

func TestUpdateAddress(t *testing.T) {
}

func loadTestData() (map[string]*scheduler.RegisterResult, map[string][]*scheduler.PeerPacket, map[string]string) {
	var normalTaskURL = "https://www.dragonfly.com"
	var smallTaskURL = "https://www.dragonfly1.com"
	var tinyTaskURL = "https://www.dragonfly2.com"

	var normalTaskID = idgen.TaskID(normalTaskURL, nil)
	var smallTaskID = idgen.TaskID(smallTaskURL, nil)
	var tinyTaskID = idgen.TaskID(tinyTaskURL, nil)

	var normalRegisterResult = &scheduler.RegisterResult{
		TaskId:      normalTaskID,
		SizeScope:   base.SizeScope_NORMAL,
		DirectPiece: nil,
	}

	var smallRegisterResult = &scheduler.RegisterResult{
		TaskId:    smallTaskID,
		SizeScope: base.SizeScope_SMALL,
		DirectPiece: &scheduler.RegisterResult_SinglePiece{
			SinglePiece: &scheduler.SinglePiece{
				DstPid:  "x",
				DstAddr: "x",
				PieceInfo: &base.PieceInfo{
					PieceNum:    1,
					RangeStart:  0,
					RangeSize:   100,
					PieceMd5:    "xxxxx",
					PieceOffset: 0,
					PieceStyle:  1,
				},
			},
		},
	}

	var tinyRegisterResult = &scheduler.RegisterResult{
		TaskId:    tinyTaskID,
		SizeScope: base.SizeScope_TINY,
		DirectPiece: &scheduler.RegisterResult_PieceContent{
			PieceContent: []byte("dragonfly2"),
		},
	}

	var taskRegisterResult = map[string]*scheduler.RegisterResult{
		normalTaskURL: normalRegisterResult,
		smallTaskURL:  smallRegisterResult,
		tinyTaskURL:   tinyRegisterResult,
	}

	var taskUrl2Id = map[string]string{
		normalTaskURL: normalTaskID,
		smallTaskURL:  smallTaskID,
		tinyTaskURL:   tinyTaskID,
	}

	var normalPeerPacket = []*scheduler.PeerPacket{
		{
			TaskId:        normalTaskID,
			SrcPid:        "peer1",
			ParallelCount: 1,
			MainPeer: &scheduler.PeerPacket_DestPeer{
				Ip:      "",
				RpcPort: 0,
				PeerId:  "",
			},
			StealPeers: []*scheduler.PeerPacket_DestPeer{},
			Code:       base.Code_Success,
		},
		{
			TaskId:        normalTaskID,
			SrcPid:        "peer1",
			ParallelCount: 1,
			MainPeer: &scheduler.PeerPacket_DestPeer{
				Ip:      "",
				RpcPort: 0,
				PeerId:  "",
			},
			StealPeers: []*scheduler.PeerPacket_DestPeer{},
			Code:       base.Code_Success,
		},
	}

	var taskInfos = map[string][]*scheduler.PeerPacket{
		normalTaskID: normalPeerPacket,
	}
	return taskRegisterResult, taskInfos, taskUrl2Id
}
