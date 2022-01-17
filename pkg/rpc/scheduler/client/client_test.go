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
	"time"

	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/serialx/hashring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/apimachinery/pkg/util/sets"
)

var _ scheduler.SchedulerServer = (*testServer)(nil)

type testServer struct {
	scheduler.UnimplementedSchedulerServer

	registerResult map[string]*scheduler.RegisterResult // url->registerResult
	taskInfo       map[string][]*scheduler.PeerPacket   // taskID-> peerPacket
	taskUrl2Id     map[string]string                    // url->taskID
	taskID2Url     map[string]string
	peerIds        sets.String
	addr           string
}

// RegisterPeerTask registers a peer into one task.
func (s *testServer) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	log.Printf("server %s receive Download request %v", s.addr, req)
	md2, ok := metadata.FromIncomingContext(ctx)
	if ok && md2.Get("excludeaddrs") != nil {
		if sets.NewString(md2.Get("excludeaddrs")...).Has(s.addr) {
			return nil, dferrors.New(base.Code_CDNTaskRegistryFail, "hit exclude server")
		}
	}
	if rs, ok := s.registerResult[req.Url]; !ok {
		return nil, dferrors.New(base.Code_CDNTaskRegistryFail, "wrong server")
	} else {
		s.peerIds.Insert(req.PeerId)
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
		_, ok := s.peerIds[in.SrcPid]
		if !ok {
			return dferrors.New(base.Code_SchedPeerNotFound, "peer not found")
		}
		if err := stream.Send(&scheduler.PeerPacket{
			TaskId:        in.TaskId,
			SrcPid:        in.SrcPid,
			ParallelCount: 1,
			MainPeer: &scheduler.PeerPacket_DestPeer{
				Ip:      "1.1.1.1",
				RpcPort: 0,
				PeerId:  "dest_peer",
			},
			StealPeers: []*scheduler.PeerPacket_DestPeer{
				{
					Ip:      "2.2.2.2",
					RpcPort: 0,
					PeerId:  "dest_peer2",
				},
			},
			Code: base.Code_Success,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReportPeerResult reports downloading result for the peer task.
func (s *testServer) ReportPeerResult(ctx context.Context, result *scheduler.PeerResult) (*emptypb.Empty, error) {
	if _, ok := s.peerIds[result.PeerId]; !ok {
		return nil, dferrors.New(base.Code_SchedPeerNotFound, "peer not found")
	}
	return &emptypb.Empty{}, nil
}

// LeaveTask makes the peer leaving from scheduling overlay for the task.
func (s *testServer) LeaveTask(ctx context.Context, target *scheduler.PeerTarget) (*emptypb.Empty, error) {
	if _, ok := s.peerIds[target.PeerId]; !ok {
		return nil, dferrors.New(base.Code_SchedPeerNotFound, "peer not found")
	}
	return &emptypb.Empty{}, nil
}

func newTestServer(addr string, registerResult map[string]*scheduler.RegisterResult) *testServer {
	s := &testServer{
		registerResult: registerResult,
		taskInfo:       make(map[string][]*scheduler.PeerPacket),
		taskUrl2Id:     make(map[string]string),
		taskID2Url:     make(map[string]string),
		peerIds:        sets.NewString(),
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
	//serverHashRing := hashring.New(t.addresses)
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		//var rs map[string]*scheduler.RegisterResult
		//if count != 1 {
		//	rs = map[string]*scheduler.RegisterResult{
		//		taskUrls[i%len(taskUrls)]: registerResult[taskUrls[i%len(taskUrls)]],
		//	}
		//} else {
		//	rs = registerResult
		//}
		sImpl := newTestServer(lis.Addr().String(), registerResult)
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
	var taskURL = normalTaskURL
	var taskID = idgen.TaskID(taskURL, nil)
	{
		// register
		taskRequest := &scheduler.PeerTaskRequest{
			Url:         taskURL,
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
		if !cmp.Equal(result, test.serverImpls[0].registerResult[taskURL], cmpopts.IgnoreUnexported(scheduler.RegisterResult{}),
			cmpopts.IgnoreUnexported(scheduler.SinglePiece{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
			t.Fatalf("registerResult is not same as expected, expected: %s, actual %s", test.serverImpls[0].registerResult[taskURL], result)
		}
	}
	{
		// start report
		taskRequest := &scheduler.PeerTaskRequest{
			Url:         taskURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}
		stream, err := client.ReportPieceResult(context.Background(), taskID, taskRequest)
		if err != nil {
			t.Fatalf("failed to call ReportPieceResult: %v", err)
		}
		waitClose := make(chan struct{})
		go func() {
			for {
				peerPacket, err := stream.Recv()
				if err == io.EOF {
					close(waitClose)
					return
				}
				if err != nil {
					t.Fatalf("failed to recive a peer packet: %v", err)
				}
				log.Printf("received a peer packet: %s", peerPacket)
			}
		}()
		pieceResults := []*scheduler.PieceResult{
			{
				TaskId: taskID,
				SrcPid: "test_peer",
				DstPid: "dstPeer",
				PieceInfo: &base.PieceInfo{
					PieceNum:    0,
					RangeStart:  0,
					RangeSize:   0,
					PieceMd5:    "xxx",
					PieceOffset: 0,
					PieceStyle:  0,
				},
				BeginTime:     0,
				EndTime:       0,
				Success:       true,
				Code:          base.Code_Success,
				HostLoad:      nil,
				FinishedCount: 1,
			}, {
				TaskId: taskID,
				SrcPid: "test_peer",
				DstPid: "dstPeer",
				PieceInfo: &base.PieceInfo{
					PieceNum:    0,
					RangeStart:  0,
					RangeSize:   0,
					PieceMd5:    "xxx",
					PieceOffset: 0,
					PieceStyle:  0,
				},
				BeginTime:     0,
				EndTime:       0,
				Success:       true,
				Code:          base.Code_Success,
				HostLoad:      nil,
				FinishedCount: 2,
			},
		}
		for _, pieceResult := range pieceResults {
			if err := stream.Send(pieceResult); err != nil {
				t.Fatalf("failed to send a piece result: %v", err)
			}
		}
		stream.CloseSend()
		<-waitClose
	}
	{
		peerResult := &scheduler.PeerResult{
			TaskId:          taskID,
			PeerId:          "test_peer",
			SrcIp:           "1.1.1.1",
			SecurityDomain:  "",
			Idc:             "",
			Url:             taskURL,
			ContentLength:   1000,
			Traffic:         1000,
			Cost:            100,
			Success:         true,
			Code:            base.Code_Success,
			TotalPieceCount: 2,
		}
		if err := client.ReportPeerResult(context.Background(), peerResult); err != nil {
			t.Fatalf("failed to report peer result: %v", err)
		}
	}
	{
		if err := client.LeaveTask(context.Background(), &scheduler.PeerTarget{
			TaskId: taskID,
			PeerId: "test_peer",
		}); err != nil {
			t.Fatalf("failed to leave task: %v", err)
		}
	}
}

func TestUpdateAddress(t *testing.T) {
	test, err := startTestServers(5)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	client, err := GetClientByAddrs([]dfnet.NetAddr{{Addr: test.addresses[0]}, {Addr: test.addresses[1]}})
	if err != nil {
		t.Fatalf("failed to get daemon client: %v", err)
	}
	defer client.Close()
	var taskURL = normalTaskURL
	var taskID = idgen.TaskID(taskURL, nil)
	serverHashRing := hashring.New(test.addresses[0:2])
	expectedServer, ok := serverHashRing.GetNode(taskID)
	if !ok {
		t.Fatalf("failed to get server node")
	}
	{
		var calledServer peer.Peer
		// register
		registerRequest := &scheduler.PeerTaskRequest{
			Url:         taskURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := client.RegisterPeerTask(ctx, registerRequest, grpc.Peer(&calledServer))
		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		if !cmp.Equal(result, test.serverImpls[0].registerResult[taskURL], cmpopts.IgnoreUnexported(scheduler.RegisterResult{}),
			cmpopts.IgnoreUnexported(scheduler.SinglePiece{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
			t.Fatalf("registerResult is not same as expected, expected: %s, actual %s", test.serverImpls[0].registerResult[taskURL], result)
		}
		if calledServer.Addr.String() != expectedServer {
			t.Fatalf("hash taskID failed, expected server: %s, actual: %s", expectedServer, calledServer.Addr)
		}
	}
	// update address should hash to same server node
	if err := client.UpdateAddresses([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]},
		{Addr: test.addresses[3]}}); err != nil {
		t.Fatalf("failed to update address: %v", err)
	}
	serverHashRing = hashring.New(test.addresses)
	xxx, _ := serverHashRing.GetNode(taskID)
	log.Printf("updated server node: %s, previous server node: %s", xxx, expectedServer)
	{
		var calledServer peer.Peer
		taskRequest := &scheduler.PeerTaskRequest{
			Url:         taskURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stream, err := client.ReportPieceResult(ctx, taskID, taskRequest, grpc.Peer(&calledServer))
		if err != nil {
			t.Fatalf("failed to call ReportPieceResult: %v", err)
		}
		waitClose := make(chan struct{})
		go func() {
			for {
				peerPacket, err := stream.Recv()
				if err == io.EOF {
					close(waitClose)
					return
				}
				if err != nil {
					t.Fatalf("failed to recive a peer packet: %v", err)
				}
				log.Printf("received a peer packet: %s", peerPacket)
			}
		}()
		pieceResults := []*scheduler.PieceResult{
			{
				TaskId: taskID,
				SrcPid: "test_peer",
				DstPid: "dstPeer",
				PieceInfo: &base.PieceInfo{
					PieceNum:    0,
					RangeStart:  0,
					RangeSize:   0,
					PieceMd5:    "xxx",
					PieceOffset: 0,
					PieceStyle:  0,
				},
				BeginTime:     0,
				EndTime:       0,
				Success:       true,
				Code:          base.Code_Success,
				HostLoad:      nil,
				FinishedCount: 1,
			}, {
				TaskId: taskID,
				SrcPid: "test_peer",
				DstPid: "dstPeer",
				PieceInfo: &base.PieceInfo{
					PieceNum:    0,
					RangeStart:  0,
					RangeSize:   0,
					PieceMd5:    "xxx",
					PieceOffset: 0,
					PieceStyle:  0,
				},
				BeginTime:     0,
				EndTime:       0,
				Success:       true,
				Code:          base.Code_Success,
				HostLoad:      nil,
				FinishedCount: 2,
			},
		}
		for _, pieceResult := range pieceResults {
			if err := stream.Send(pieceResult); err != nil {
				t.Fatalf("failed to send a piece result: %v", err)
			}
		}
		stream.CloseSend()
		<-waitClose
		if calledServer.Addr.String() != expectedServer {
			t.Fatalf("hash taskID failed, expected server: %s, actual: %s", expectedServer, calledServer.Addr)
		}
	}
	// remove server2 and add server 4 should hash to same server node
	if err := client.UpdateAddresses([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[3]},
		{Addr: test.addresses[4]}}); err != nil {
		t.Fatalf("failed to update address: %v", err)
	}
	serverHashRing = hashring.New(test.addresses)
	xxx, _ = serverHashRing.GetNode(taskID)
	log.Printf("updated server node: %s, previous server node: %s", xxx, expectedServer)
	{
		var calledServer peer.Peer
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := client.ReportPeerResult(ctx, &scheduler.PeerResult{
			TaskId:          taskID,
			PeerId:          "test_peer",
			SrcIp:           "1.1.1.1",
			Url:             taskURL,
			ContentLength:   0,
			Traffic:         0,
			Cost:            0,
			Success:         true,
			Code:            base.Code_Success,
			TotalPieceCount: 0,
		}, grpc.Peer(&calledServer))
		if err != nil {
			t.Fatalf("failed to call reportPeerResult: %v", err)
		}
		if calledServer.Addr.String() != expectedServer {
			t.Fatalf("hash taskID failed, expected server: %s, actual: %s", expectedServer, calledServer.Addr)
		}
	}
	// remove all server nodes,and add an invalid addr, leave method call should failed
	if err := client.UpdateAddresses([]dfnet.NetAddr{{Addr: "1.1.1.1:8080"}}); err != nil {
		t.Fatalf("failed to update address: %v", err)
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := client.LeaveTask(ctx, &scheduler.PeerTarget{
			TaskId: taskID,
			PeerId: "test_peer",
		})
		if status.Code(err) != codes.Code(base.Code_ServerUnavailable) {
			t.Fatalf("leave task call should return base.Code_ServerUnavailable but got: %v", err)
		}
	}
}

func TestMigration(t *testing.T) {
	test, err := startTestServers(3)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	client, err := GetClientByAddrs([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]}})
	if err != nil {
		t.Fatalf("failed to get scheduler client: %v", err)
	}
	defer client.Close()
	{
		// exclude len(serverHashRing)-1 server nodes, only left one
		var testTaskURL = normalTaskURL
		testTaskID := idgen.TaskID(testTaskURL, nil)
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(testTaskID, len(test.servers))
		var excludeAddrs []string
		for _, addr := range candidateAddrs[0 : len(candidateAddrs)-1] {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		var serverPeer peer.Peer
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		result, err := client.RegisterPeerTask(ctx, &scheduler.PeerTaskRequest{
			Url:         testTaskURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}, grpc.Peer(&serverPeer))
		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		if !cmp.Equal(result, test.serverImpls[0].registerResult[testTaskURL], cmpopts.IgnoreUnexported(scheduler.RegisterResult{}),
			cmpopts.IgnoreUnexported(scheduler.SinglePiece{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
			t.Fatalf("registerResult is not same as expected, expected: %s, actual %s", test.serverImpls[0].registerResult[testTaskURL], result)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}

		// report peer should hit migrated server node
		err = client.ReportPeerResult(context.Background(), &scheduler.PeerResult{
			TaskId:  testTaskID,
			PeerId:  "test_peer",
			SrcIp:   "1.1.1.1",
			Url:     testTaskURL,
			Success: true,
			Code:    base.Code_Success,
		}, grpc.Peer(&serverPeer))
		if err != nil {
			t.Fatalf("failed to call ReportPeerResult: %v", err)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}

	{
		// all server node failed
		var testTaskURL = normalTaskURL
		testTaskID := idgen.TaskID(testTaskURL, nil)
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(testTaskID, len(test.servers))
		var serverPeer peer.Peer
		var excludeAddrs []string
		for _, addr := range candidateAddrs {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		_, err := client.RegisterPeerTask(ctx, &scheduler.PeerTaskRequest{
			Url:         testTaskURL,
			UrlMeta:     nil,
			PeerId:      "test_peer",
			PeerHost:    nil,
			HostLoad:    nil,
			IsMigrating: false,
		}, grpc.Peer(&serverPeer))
		if err == nil || !cmp.Equal(err.Error(), status.New(codes.Unknown, dferrors.New(base.Code_CDNTaskRegistryFail, "hit exclude server").Error()).String()) {
			t.Fatalf("RegisterPeerTask want err hit exclude server, but got %v", err)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}
}

var normalTaskURL = "https://www.dragonfly.com"
var smallTaskURL = "https://www.dragonfly1.com"
var tinyTaskURL = "https://www.dragonfly2.com"

func loadTestData() (map[string]*scheduler.RegisterResult, map[string][]*scheduler.PeerPacket, map[string]string) {
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
