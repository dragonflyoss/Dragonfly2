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
	"log"
	"net"
	"testing"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
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

	registerResult map[string]*scheduler.RegisterResult // url->piecePacket
	addr           string
}

// RegisterPeerTask registers a peer into one task.
func (s *testServer) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	if rs, ok := s.registerResult[req.Url]; !ok {
		return nil, status.Error(codes.NotFound, "wrong server")
	} else {
		return rs, nil
	}
}

// ReportPieceResult reports piece results and receives peer packets.
// when migrating to another scheduler,
// it will send the last piece result to the new scheduler.
func (s *testServer) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	return nil
}

// ReportPeerResult reports downloading result for the peer task.
func (s *testServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (*emptypb.Empty, error) {
	return nil, nil
}

// LeaveTask makes the peer leaving from scheduling overlay for the task.
func (s *testServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (*emptypb.Empty, error) {
	return nil, nil
}

func loadTestData() (registerResult map[string]*scheduler.RegisterResult, progress map[string][]*dfdaemon.DownResult) {
	registerResult = taskRegisterResult
	return
}

func newTestServer(addr string, registerResult map[string]*scheduler.RegisterResult) *testServer {
	s := &testServer{addr: addr, registerResult: registerResult}
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
	registerResult, _ := loadTestData()
	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	var tasks []string
	for s, _ := range registerResult {
		tasks = append(tasks, s)
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
				tasks[i%len(tasks)]: registerResult[tasks[i%len(tasks)]],
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
		if !cmp.Equal(result, taskRegisterResult[testURL], cmpopts.IgnoreUnexported(scheduler.RegisterResult{}),
			cmpopts.IgnoreUnexported(scheduler.SinglePiece{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
			t.Fatalf("registerResult is not same as expected, expected: %s, actual %s", taskRegisterResult[testURL], result)
		}
	}
}

var normalTestTask = &scheduler.RegisterResult{
	TaskId:      "dragonfly",
	SizeScope:   base.SizeScope_NORMAL,
	DirectPiece: nil,
}

var smallTestTask = &scheduler.RegisterResult{
	TaskId:    "dragonfly1",
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

var tinyTestTask = &scheduler.RegisterResult{
	TaskId:    "dragonfly2",
	SizeScope: base.SizeScope_TINY,
	DirectPiece: &scheduler.RegisterResult_PieceContent{
		PieceContent: []byte("dragonfly2"),
	},
}

var taskRegisterResult = map[string]*scheduler.RegisterResult{
	"https://www.dragonfly.com":  normalTestTask,
	"https://www.dragonfly1.com": smallTestTask,
	"https://www.dragonfly2.com": tinyTestTask,
}
