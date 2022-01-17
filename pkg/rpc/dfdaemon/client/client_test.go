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

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
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
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
)

var _ dfdaemon.DaemonServer = (*testServer)(nil)

type testServer struct {
	dfdaemon.UnimplementedDaemonServer

	progress    map[string][]*dfdaemon.DownResult // url-> downloadResult
	piecePacket map[string]*base.PiecePacket      // taskID->piecePacket
	addr        string
}

func (s *testServer) Download(request *dfdaemon.DownRequest, stream dfdaemon.Daemon_DownloadServer) error {
	log.Printf("server %s receive Download request %v", s.addr, request)
	md2, ok := metadata.FromIncomingContext(stream.Context())
	if ok && md2.Get("excludeaddrs") != nil {
		if sets.NewString(md2.Get("excludeaddrs")...).Has(s.addr) {
			return status.Errorf(codes.NotFound, "task not found")
		}
	}
	testURL := request.Url
	if strings.HasPrefix(testURL, "https://www.dragonfly1.com") {
		testURL = "https://www.dragonfly1.com"
	}
	if strings.HasPrefix(testURL, "https://www.dragonfly2.com") {
		testURL = "https://www.dragonfly1.com"
	}
	pieceInfos, ok := s.progress[testURL]
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

func (s *testServer) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	log.Printf("server %s receive get piece task request %v", s.addr, request)
	if strings.HasPrefix(request.TaskId, "aaaaaaaaaaaaaaa") {
		request.TaskId = "aaaaaaaaaaaaaaa"
	}
	if strings.HasPrefix(request.TaskId, "bbbbbbbbbbbbbbb") {
		request.TaskId = "bbbbbbbbbbbbbbb"
	}
	piecePacket, ok := s.piecePacket[request.TaskId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task not found")
	}
	pieceInfos := make([]*base.PieceInfo, 0)
	var count uint32 = 0
	for _, info := range piecePacket.PieceInfos {
		if uint32(info.PieceNum) >= request.StartNum && (count < request.Limit || request.Limit <= 0) {
			p := &base.PieceInfo{
				PieceNum:    info.PieceNum,
				RangeStart:  info.RangeStart,
				RangeSize:   info.RangeSize,
				PieceMd5:    info.PieceMd5,
				PieceOffset: info.PieceOffset,
				PieceStyle:  info.PieceStyle,
			}
			pieceInfos = append(pieceInfos, p)
			count++
		}
	}
	pp := &base.PiecePacket{
		TaskId:        request.TaskId,
		DstPid:        request.DstPid,
		DstAddr:       piecePacket.DstAddr,
		PieceInfos:    pieceInfos,
		TotalPiece:    piecePacket.TotalPiece,
		ContentLength: piecePacket.ContentLength,
		PieceMd5Sign:  piecePacket.PieceMd5Sign,
	}
	return pp, nil
}

func (s *testServer) CheckHealth(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func loadTestData() (piecePacket map[string]*base.PiecePacket, progress map[string][]*dfdaemon.DownResult) {
	progressData, err := os.ReadFile("../testdata/download_progress.json")
	if err != nil {
		log.Fatalf("failed to load progress info: %v", err)
	}
	if err := json.Unmarshal(progressData, &progress); err != nil {
		log.Fatalf("failed to load progress info: %v", err)
	}
	piecesData, err := os.ReadFile("../testdata/pieces.json")
	if err != nil {
		log.Fatalf("failed to load seed piece info: %v", err)
	}
	if err := json.Unmarshal(piecesData, &piecePacket); err != nil {
		log.Fatalf("failed to load piece info: %v", err)
	}
	return
}

func newTestServer(addr string, piecePacket map[string]*base.PiecePacket, progress map[string][]*dfdaemon.DownResult) *testServer {
	s := &testServer{addr: addr, piecePacket: piecePacket, progress: progress}
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
	pieces, progress := loadTestData()
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
		sImpl := newTestServer(lis.Addr().String(), pieces, progress)
		dfdaemon.RegisterDaemonServer(s, sImpl)
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
	progressInfos := test.serverImpls[0].progress[testURL]
	{
		downReq := &dfdaemon.DownRequest{
			Url:               testURL,
			Output:            "",
			Timeout:           0,
			Limit:             0,
			DisableBackSource: false,
			UrlMeta:           nil,
			Pattern:           "",
			Callsystem:        "",
		}
		stream, err := client.Download(context.Background(), downReq)
		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		var index int32
		for {
			progress, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("failed to recv progress info: %v", err)
			}
			if !cmp.Equal(progress, progressInfos[index], cmpopts.IgnoreUnexported(dfdaemon.DownResult{})) {
				t.Fatalf("failed to verify progress, want:%v, actual: %v", progressInfos[index], progress)
			}
			index++
		}
	}

	{
		testTaskID := "aaaaaaaaaaaaaaa"
		req := &base.PieceTaskRequest{
			TaskId:   testTaskID,
			SrcPid:   "peer1",
			DstPid:   "peer2",
			StartNum: 0,
			Limit:    3,
		}
		piecePacket, err := client.GetPieceTasks(context.Background(), dfnet.NetAddr{Addr: test.addresses[0]}, req)

		if err != nil {
			t.Fatalf("failed to call GetPieceTasks: %v", err)
		}
		if len(piecePacket.PieceInfos) != int(req.Limit) {
			t.Fatalf("piece count is not same with req, want: %d, actual: %d", req.Limit, len(piecePacket.PieceInfos))
		}
		verifyPieces := test.serverImpls[0].piecePacket[testTaskID].PieceInfos
		var pieceIndex = 0
		for {
			if !cmp.Equal(piecePacket.PieceInfos[pieceIndex], verifyPieces[pieceIndex], cmpopts.IgnoreUnexported(base.PieceInfo{})) {
				t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[pieceIndex], piecePacket.PieceInfos[pieceIndex])
			}
			pieceIndex++
			if pieceIndex == int(req.Limit) {
				break
			}
		}
	}
	{
		err := client.CheckHealth(context.Background(), dfnet.NetAddr{Addr: test.addresses[0]})
		if err != nil {
			t.Fatalf("check health should ok, but got err: %v", err)
		}
	}
}

func TestHashTask(t *testing.T) {
	test, err := startTestServers(3)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	client, err := GetClientByAddrs([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]},
	})
	if err != nil {
		t.Fatalf("failed to get dfdaemon client: %v", err)
	}
	defer client.Close()
	serverHashRing := hashring.New(test.addresses)
	{
		var taskID = idgen.TaskID("https://www.dragonfly1.com", nil)
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
				stream, err := client.Download(context.Background(), &dfdaemon.DownRequest{
					Url:     "https://www.dragonfly1.com",
					UrlMeta: nil,
				}, grpc.Peer(&temp))
				if err != nil {
					return errors.Errorf("failed to call Download: %v", err)
				}
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Errorf("failed to recv Download: %v", err)
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
			t.Fatalf("failed to call Download: %v", err)
		}
		close(serverPeerChan)
		wg.Wait()
		if serverPeerCount != totalCallCount {
			t.Fatalf("call Download success count is wrong, expect %d actual %d", totalCallCount, serverPeerCount)
		}
		for _, addr := range serverAddrs {
			if selectedServer != addr {
				t.Fatalf("failed to hash to same server, want %s, actual %s", selectedServer, addr)
			}
		}
	}
	{
		var taskURL1 = "https://www.dragonfly1.com"
		var taskURL2 = "https://www.dragonfly2.com"
		selectedServer1, _ := serverHashRing.GetNode(idgen.TaskID(taskURL1, nil))
		selectedServer2, _ := serverHashRing.GetNode(idgen.TaskID(taskURL2, nil))
		for {
			if selectedServer1 != selectedServer2 {
				break
			}
			taskURL1 = taskURL1 + rand.String(3)
			taskURL2 = taskURL2 + rand.String(3)
			selectedServer1, _ = serverHashRing.GetNode(idgen.TaskID(taskURL1, nil))
			selectedServer2, _ = serverHashRing.GetNode(idgen.TaskID(taskURL2, nil))
		}
		// test hash different taskID to different server node
		var serverPeer1 peer.Peer
		var serverPeer2 peer.Peer
		eg := errgroup.Group{}
		eg.Go(func() error {
			stream, err := client.Download(context.Background(), &dfdaemon.DownRequest{
				Url:     taskURL1,
				UrlMeta: nil,
			}, grpc.Peer(&serverPeer1))
			if err != nil {
				return errors.Errorf("failed to call Download: %v", err)
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Errorf("failed to recv Download: %v", err)
				}
			}
			return nil
		})
		eg.Go(func() error {
			stream, err := client.Download(context.Background(), &dfdaemon.DownRequest{
				Url:     taskURL2,
				UrlMeta: nil,
			}, grpc.Peer(&serverPeer2))
			if err != nil {
				return errors.Errorf("failed to call Download: %v", err)
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Errorf("failed to recv Download: %v", err)
				}
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatalf("failed to call Download: %v", err)
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
		t.Fatalf("failed to get start servers: %v", err)
	}
	defer test.cleanup()

	client, err := GetClientByAddrs([]dfnet.NetAddr{
		{Addr: test.addresses[0]},
		{Addr: test.addresses[1]},
		{Addr: test.addresses[2]}})
	if err != nil {
		t.Fatalf("failed to get dfdaemon client: %v", err)
	}
	defer client.Close()
	{
		// exclude len(serverHashRing)-1 server nodes, only left one
		var testTaskURL = "https://www.dragonfly1.com"
		testTaskID := idgen.TaskID(testTaskURL, nil)
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(testTaskID, len(test.servers))
		var excludeAddrs []string
		for _, addr := range candidateAddrs[0 : len(candidateAddrs)-1] {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		var serverPeer peer.Peer
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		stream, err := client.Download(ctx, &dfdaemon.DownRequest{
			Url:     testTaskURL,
			UrlMeta: nil,
		}, grpc.Peer(&serverPeer))

		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("failed to recieve Download: %v", err)
			}
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}

	{
		// all server node failed
		var testTaskURL = "https://www.dragonfly1.com"
		testTaskID := idgen.TaskID(testTaskURL, nil)
		serverHashRing := hashring.New(test.addresses)
		candidateAddrs, _ := serverHashRing.GetNodes(testTaskID, len(test.servers))
		var serverPeer peer.Peer
		var excludeAddrs []string
		for _, addr := range candidateAddrs {
			excludeAddrs = append(excludeAddrs, "excludeAddrs", addr)
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(excludeAddrs...))
		stream, err := client.Download(ctx, &dfdaemon.DownRequest{
			Url:     "https://www.dragonfly1.com",
			UrlMeta: nil,
		}, grpc.Peer(&serverPeer))

		if err != nil {
			t.Fatalf("failed to call Download: %v", err)
		}
		_, err = stream.Recv()
		if err == nil || !cmp.Equal(err.Error(), status.Errorf(codes.NotFound, "task not found").Error()) {
			t.Fatalf("download want err task not found, but got %v", err)
		}
		if serverPeer.Addr.String() != candidateAddrs[len(candidateAddrs)-1] {
			t.Fatalf("target server addr is not same as expected, want: %s, actual: %s", candidateAddrs[len(candidateAddrs)-1], serverPeer.Addr.String())
		}
	}
}
