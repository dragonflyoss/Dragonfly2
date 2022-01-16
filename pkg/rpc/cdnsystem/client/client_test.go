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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/serialx/hashring"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
)

var _ cdnsystem.SeederServer = (*testServer)(nil)

type testServer struct {
	cdnsystem.UnimplementedSeederServer

	seedPieces map[string][]*cdnsystem.PieceSeed
	addr       string
}

func loadPieces() map[string][]*cdnsystem.PieceSeed {
	var seedPieces map[string][]*cdnsystem.PieceSeed
	data, err := os.ReadFile("../testdata/seed_piece_info.json")
	if err != nil {
		log.Fatalf("failed to load seed piece info: %v", err)
	}
	if err := json.Unmarshal(data, &seedPieces); err != nil {
		log.Fatalf("failed to load piece info: %v", err)
	}
	return seedPieces
}

func (s *testServer) ObtainSeeds(request *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) error {
	log.Printf("server %s receive obtain request %v", s.addr, request)
	md2, ok := metadata.FromIncomingContext(stream.Context())
	if ok && md2.Get("excludeaddrs") != nil {
		if sets.NewString(md2.Get("excludeaddrs")...).Has(s.addr) {
			return status.Errorf(codes.NotFound, "task not found")
		}
	}
	if strings.HasPrefix(request.TaskId, "aaaaaaaaaaaaaaa") {
		request.TaskId = "aaaaaaaaaaaaaaa"
	}
	if strings.HasPrefix(request.TaskId, "bbbbbbbbbbbbbbb") {
		request.TaskId = "bbbbbbbbbbbbbbb"
	}
	pieceInfos, ok := s.seedPieces[request.TaskId]
	if !ok {
		return status.Errorf(codes.NotFound, "task not found")
	}
	for _, info := range pieceInfos {
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

func (s *testServer) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	log.Printf("server %s receive get piece task request %v", s.addr, req)
	if strings.HasPrefix(req.TaskId, "aaaaaaaaaaaaaaa") {
		req.TaskId = "aaaaaaaaaaaaaaa"
	}
	if strings.HasPrefix(req.TaskId, "bbbbbbbbbbbbbbb") {
		req.TaskId = "bbbbbbbbbbbbbbb"
	}
	pieces, ok := s.seedPieces[req.TaskId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task not found")
	}
	pieceInfos := make([]*base.PieceInfo, 0, len(pieces))
	var count uint32 = 0
	for _, info := range pieces {
		if uint32(info.PieceInfo.PieceNum) >= req.StartNum && (count < req.Limit || req.Limit <= 0) {
			p := &base.PieceInfo{
				PieceNum:    info.PieceInfo.PieceNum,
				RangeStart:  info.PieceInfo.RangeStart,
				RangeSize:   info.PieceInfo.RangeSize,
				PieceMd5:    info.PieceInfo.PieceMd5,
				PieceOffset: info.PieceInfo.PieceOffset,
				PieceStyle:  info.PieceInfo.PieceStyle,
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	pp := &base.PiecePacket{
		TaskId:        req.TaskId,
		DstPid:        req.DstPid,
		DstAddr:       fmt.Sprintf("%s:%s", "127.0.0.1", "8080"),
		PieceInfos:    pieceInfos,
		TotalPiece:    pieces[len(pieces)-1].TotalPieceCount,
		ContentLength: pieces[len(pieces)-1].ContentLength,
		PieceMd5Sign:  "",
	}
	return pp, nil
}

func newTestServer(addr string, seedPieces map[string][]*cdnsystem.PieceSeed) *testServer {
	s := &testServer{addr: addr, seedPieces: seedPieces}
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
	seedPieces := loadPieces()
	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestServer(lis.Addr().String(), seedPieces)
		cdnsystem.RegisterSeederServer(s, sImpl)
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

	cdnClient, err := GetClientByAddrs([]dfnet.NetAddr{{Addr: test.addresses[0]}})
	if err != nil {
		t.Fatalf("failed to get cdn client: %v", err)
	}
	defer cdnClient.Close()

	var testTask = "aaaaaaaaaaaaaaa"
	verifyPieces := test.serverImpls[0].seedPieces[testTask]
	{
		obtainReq := &cdnsystem.SeedRequest{
			TaskId:  testTask,
			Url:     "https://dragonfly.com",
			UrlMeta: nil,
		}
		stream, err := cdnClient.ObtainSeeds(context.Background(), obtainReq)
		if err != nil {
			t.Fatalf("failed to call ObtainSeeds: %v", err)
		}
		var pieceIndex int32
		for {
			piece, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("failed to recv piece info: %v", err)
			}
			if !cmp.Equal(piece, verifyPieces[pieceIndex], cmpopts.IgnoreUnexported(cdnsystem.PieceSeed{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
				t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[pieceIndex], piece)
			}
			pieceIndex++
		}
	}

	{
		req := &base.PieceTaskRequest{
			TaskId:   testTask,
			SrcPid:   "peer1",
			DstPid:   "peer2",
			StartNum: 0,
			Limit:    1,
		}
		piecePacket, err := cdnClient.GetPieceTasks(context.Background(), dfnet.NetAddr{Addr: test.addresses[0]}, req)
		if err != nil {
			t.Fatalf("failed to call GetPieceTasks: %v", err)
		}
		if len(piecePacket.PieceInfos) != int(req.Limit) {
			t.Fatalf("piece count is not same with req, want: %d, actual: %d", req.Limit, len(piecePacket.PieceInfos))
		}
		var pieceIndex = 0
		for {
			if !cmp.Equal(piecePacket.PieceInfos[pieceIndex], verifyPieces[pieceIndex].PieceInfo, cmpopts.IgnoreUnexported(cdnsystem.PieceSeed{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
				t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[pieceIndex].PieceInfo, piecePacket.PieceInfos[pieceIndex])
			}
			pieceIndex++
			if pieceIndex == int(req.Limit) {
				break
			}
		}
	}
}

func TestHashTask(t *testing.T) {
	test, err := startTestServers(3)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cdnClient, err := GetClientByAddrs([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]},
	})
	if err != nil {
		t.Fatalf("failed to get cdn client: %v", err)
	}
	defer cdnClient.Close()
	serverHashRing := hashring.New(test.addresses)
	{
		var taskID = "aaaaaaaaaaaaaaa"
		selectedServer, ok := serverHashRing.GetNode(taskID)
		if !ok {
			t.Fatalf("failed to hash server node for task: %s", taskID)
		}
		// test hash same taskID to same server node
		var serverPeerChan = make(chan peer.Peer)
		eg := errgroup.Group{}
		var totalCallCount = 100
		for i := 0; i < totalCallCount; i++ {
			eg.Go(func() error {
				var temp peer.Peer
				stream, err := cdnClient.ObtainSeeds(context.Background(), &cdnsystem.SeedRequest{
					TaskId:  taskID,
					Url:     "https://dragonfly.com",
					UrlMeta: nil,
				}, grpc.Peer(&temp))
				if err != nil {
					return errors.Errorf("failed to call ObtainSeeds: %v", err)
				}
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Errorf("failed to recv ObtainSeeds: %v", err)
					}
				}
				serverPeerChan <- temp
				return nil
			})
		}
		var serverAddrs []string
		serverPeerCount := 0
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range serverPeerChan {
				serverAddrs = append(serverAddrs, p.Addr.String())
				serverPeerCount++
			}
		}()
		if err := eg.Wait(); err != nil {
			t.Fatalf("failed to call obtainSeeds: %v", err)
		}
		close(serverPeerChan)
		wg.Wait()
		if serverPeerCount != totalCallCount {
			t.Fatalf("call obtainSeeds success count is wrong, expect %d actual %d", totalCallCount, serverPeerCount)
		}
		for _, addr := range serverAddrs {
			if selectedServer != addr {
				t.Fatalf("failed to hash to same server, want %s, actual %s", selectedServer, addr)
			}
		}
	}
	{
		var task1 = "aaaaaaaaaaaaaaa"
		var task2 = "bbbbbbbbbbbbbbb"
		selectedServer1, _ := serverHashRing.GetNode(task1)
		selectedServer2, _ := serverHashRing.GetNode(task2)
		for {
			if selectedServer1 != selectedServer2 {
				break
			}
			task1 = task1 + rand.String(3)
			task2 = task2 + rand.String(3)
			selectedServer1, _ = serverHashRing.GetNode(task1)
			selectedServer2, _ = serverHashRing.GetNode(task2)
		}
		// test hash different taskID to different server node
		var serverPeer1 peer.Peer
		var serverPeer2 peer.Peer
		eg := errgroup.Group{}
		eg.Go(func() error {
			stream, err := cdnClient.ObtainSeeds(context.Background(), &cdnsystem.SeedRequest{
				TaskId:  task1,
				Url:     "https://dragonfly.com",
				UrlMeta: nil,
			}, grpc.Peer(&serverPeer1))
			if err != nil {
				return errors.Errorf("failed to call ObtainSeeds: %v", err)
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Errorf("failed to recv ObtainSeeds: %v", err)
				}
			}
			return nil
		})
		eg.Go(func() error {
			stream, err := cdnClient.ObtainSeeds(context.Background(), &cdnsystem.SeedRequest{
				TaskId:  task2,
				Url:     "https://dragonfly2.com",
				UrlMeta: nil,
			}, grpc.Peer(&serverPeer2))
			if err != nil {
				return errors.Errorf("failed to call ObtainSeeds: %v", err)
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Errorf("failed to recv ObtainSeeds: %v", err)
				}
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatalf("failed to call obtainSeeds: %v", err)
		}
		if serverPeer1.Addr.String() != selectedServer1 {
			t.Fatalf("failed to hash to expected server, want %s, actual %s", selectedServer1, serverPeer1.Addr.String())
		}
		if serverPeer2.Addr.String() != selectedServer2 {
			t.Fatalf("failed to hash to expected server, want %s, actual %s", selectedServer2, serverPeer2.Addr.String())
		}
		if serverPeer1.Addr.String() == serverPeer2.Addr.String() {
			t.Fatalf("failed to hash to same server, server1 %s, server2 %s", serverPeer1.Addr, serverPeer2.Addr)
		}
	}
}

func TestMigration(t *testing.T) {
	test, err := startTestServers(3)
	if err != nil {
		t.Fatalf("failed to get cdn client: %v", err)
	}
	defer test.cleanup()

	cdnClient, err := GetClientByAddrs([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]}},
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 1 * time.Second,
		}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cdnClient.Close()
	{
		// exclude len(serverHashRing)-1 server nodes, only left one
		var taskID = "aaaaaaaaaaaaaaa"
		verifyPieces := test.serverImpls[0].seedPieces[taskID]
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(taskID, len(test.servers))
		var serverPeer peer.Peer
		var excludeAddrs []string
		for _, addr := range candidateAddrs[0 : len(candidateAddrs)-1] {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		stream, err := cdnClient.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
			TaskId:  taskID,
			Url:     "https://dragonfly.com",
			UrlMeta: nil,
		}, grpc.Peer(&serverPeer))

		if err != nil {
			t.Fatalf("failed to call ObtainSeeds: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("failed to recieve ObtainSeeds: %v", err)
			}
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
		req := &base.PieceTaskRequest{
			TaskId:   taskID,
			SrcPid:   "peer1",
			DstPid:   "peer2",
			StartNum: 0,
			Limit:    5,
		}
		piecePacket, err := cdnClient.GetPieceTasks(context.Background(), dfnet.NetAddr{Addr: serverPeer.Addr.String()}, req)
		if err != nil {
			t.Fatalf("failed to call GetPieceTasks: %v", err)
		}
		if len(piecePacket.PieceInfos) != int(req.Limit) {
			t.Fatalf("piece count is not same with req, want: %d, actual: %d", req.Limit, len(piecePacket.PieceInfos))
		}
		var pieceIndex = 0
		for {
			if !cmp.Equal(piecePacket.PieceInfos[pieceIndex], verifyPieces[pieceIndex].PieceInfo, cmpopts.IgnoreUnexported(cdnsystem.PieceSeed{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
				t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[pieceIndex].PieceInfo, piecePacket.PieceInfos[pieceIndex])
			}
			pieceIndex++
			if pieceIndex == int(req.Limit) {
				break
			}
		}
	}
	{
		// all server node failed
		var taskID = "aaaaaaaaaaaaaaa"
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(taskID, len(test.servers))
		var serverPeer peer.Peer
		var excludeAddrs []string
		for _, addr := range candidateAddrs {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		stream, err := cdnClient.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
			TaskId:  taskID,
			Url:     "https://dragonfly.com",
			UrlMeta: nil,
		}, grpc.Peer(&serverPeer))

		if err != nil {
			t.Fatalf("failed to call ObtainSeeds: %v", err)
		}
		_, err = stream.Recv()
		if err == nil || !cmp.Equal(err.Error(), status.Errorf(codes.NotFound, "task not found").Error()) {
			t.Fatalf("obtain seeds want err task not found, but got %v", err)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}
	{
		// add some unavailable servers
		cdnClient.UpdateAddresses(append([]dfnet.NetAddr{
			{Addr: test.addresses[0]},
			{Addr: test.addresses[1]},
			{Addr: test.addresses[2]},
			{Addr: "2.2.2.2:88"}, {Addr: "4.4.4.4:88"}}))
		// all server node failed
		var taskID = "aaaaaaaaaaaaaaa"
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(taskID, len(test.servers))
		var serverPeer peer.Peer
		var excludeAddrs []string
		for _, addr := range candidateAddrs {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		stream, err := cdnClient.ObtainSeeds(ctx, &cdnsystem.SeedRequest{
			TaskId:  taskID,
			Url:     "https://dragonfly.com",
			UrlMeta: nil,
		}, grpc.Peer(&serverPeer))

		if err != nil {
			t.Fatalf("failed to call ObtainSeeds: %v", err)
		}
		_, err = stream.Recv()
		if err == nil || !cmp.Equal(err.Error(), status.Errorf(codes.NotFound, "task not found").Error()) {
			t.Fatalf("obtain seeds want err task not found, but got %v", err)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}
}
