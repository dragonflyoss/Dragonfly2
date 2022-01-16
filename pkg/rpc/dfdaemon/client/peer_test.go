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
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ dfdaemon.DaemonServer = (*pieceTaskServer)(nil)
var _ cdnsystem.SeederServer = (*pieceTaskServer)(nil)

type pieceTaskServer struct {
	dfdaemon.UnimplementedDaemonServer
	cdnsystem.UnimplementedSeederServer

	piecePacket map[string]*base.PiecePacket // taskID->piecePacket
	addr        string
}

func (s *pieceTaskServer) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
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
	piecePacket.TaskId = request.TaskId
	piecePacket.DstPid = request.DstPid
	piecePacket.DstAddr = s.addr
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

func loadPiecePacketData() (piecePacket map[string]*base.PiecePacket) {
	piecesData, err := os.ReadFile("../testdata/pieces.json")
	if err != nil {
		log.Fatalf("failed to load seed piece info: %v", err)
	}
	if err := json.Unmarshal(piecesData, &piecePacket); err != nil {
		log.Fatalf("failed to load piece info: %v", err)
	}
	return
}

func newTestPieceTaskServer(addr string, piecePacket map[string]*base.PiecePacket) *pieceTaskServer {
	s := &pieceTaskServer{addr: addr, piecePacket: piecePacket}
	return s
}

type pieceTaskServerData struct {
	servers           []*grpc.Server
	daemonServerImpls []*pieceTaskServer
	cdnServerImpls    []*pieceTaskServer
	addresses         []string
}

func (t *pieceTaskServerData) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestPieceTaskServers(daemonServerCount, cdnServerCount int) (_ *pieceTaskServerData, err error) {
	t := &pieceTaskServerData{}
	piecePackets := loadPiecePacketData()
	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	// start daemonServerCount daemon server
	for i := 0; i < daemonServerCount; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestPieceTaskServer(lis.Addr().String(), piecePackets)
		dfdaemon.RegisterDaemonServer(s, sImpl)
		t.servers = append(t.servers, s)
		t.daemonServerImpls = append(t.daemonServerImpls, sImpl)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}
	// start cdnServerCount cdn server
	for i := 0; i < cdnServerCount; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestPieceTaskServer(lis.Addr().String(), piecePackets)
		cdnsystem.RegisterSeederServer(s, sImpl)
		t.servers = append(t.servers, s)
		t.cdnServerImpls = append(t.cdnServerImpls, sImpl)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return t, nil
}

func TestElasticClient(t *testing.T) {
	testServers, err := startTestPieceTaskServers(5, 5)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer testServers.cleanup()

	client, err := GetElasticClient(grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: 1 * time.Second,
	}))
	if err != nil {
		t.Fatalf("failed to get daemon client: %v", err)
	}
	defer client.Close()

	var server *server
	var testTaskID string

	// get piece tasks from valid cdn, should success
	testTaskID = "aaaaaaaaaaaaaaa"
	server = resolveServer(testServers.cdnServerImpls[0].addr)
	var startNum, limit uint32 = 0, 3
	piecePacket, err := client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      server.ip,
		RpcPort: server.port,
		PeerId:  "xxx_CDN",
	}, &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: startNum,
		Limit:    limit,
	})
	if err != nil {
		t.Fatalf("failed to get piece tasks from cdn %s, err: %v", server, err)
	}
	wantPiecePacket := testServers.cdnServerImpls[0].piecePacket[testTaskID]
	if !cmp.Equal(piecePacket, wantPiecePacket, cmpopts.IgnoreUnexported(base.PiecePacket{}), cmpopts.IgnoreFields(base.PiecePacket{}, "PieceInfos")) {
		t.Fatalf("piece tasks is not same with expected, expected piece tasks: %v, actual: %v", wantPiecePacket, piecePacket)
	}
	if !cmp.Equal(piecePacket.PieceInfos, wantPiecePacket.PieceInfos[startNum:limit], cmpopts.IgnoreTypes(base.PieceInfo{})) {
		t.Fatalf("piece tasks is not same with expected, expected piece tasks: %v, actual: %v", wantPiecePacket.PieceInfos[startNum:limit], piecePacket.PieceInfos)
	}

	// get piece tasks from valid daemon, should success
	testTaskID = "bbbbbbbbbbbbbbb"
	server = resolveServer(testServers.daemonServerImpls[0].addr)
	piecePacket, err = client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      server.ip,
		RpcPort: server.port,
		PeerId:  "xxxx",
	}, &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: 0,
		Limit:    0,
	})
	if err != nil {
		t.Fatalf("failed to get piece tasks from dfdaemon %s, err: %v", server, err)
	}
	wantPiecePacket = testServers.daemonServerImpls[0].piecePacket[testTaskID]
	if !cmp.Equal(piecePacket, wantPiecePacket, cmpopts.IgnoreUnexported(base.PiecePacket{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
		t.Fatalf("piece packet is not same with expected, expected piece tasks: %v, actual: %v", wantPiecePacket, piecePacket)
	}

	// get piece tasks from a nonexistent cdn, should fail
	_, err = client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      "1.1.1.1",
		RpcPort: 80,
		PeerId:  "xxx_CDN",
	}, &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: 0,
		Limit:    0,
	})
	if err == nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("get piece tasks should return unavailable error, but got: %v", err)
	}

	// get piece tasks from a nonexistent cdn, should fail
	_, err = client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      "2.2.2.2",
		RpcPort: 80,
		PeerId:  "xxx_CDN",
	}, &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: 0,
		Limit:    3,
	})
	if err == nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("get piece tasks should return unavailable error, but got: %v", err)
	}

	// get piece tasks from valid daemon, should success
	testTaskID = "bbbbbbbbbbbbbbb"
	server = resolveServer(testServers.daemonServerImpls[3].addr)
	piecePacket, err = client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      server.ip,
		RpcPort: server.port,
		PeerId:  "xxxx",
	}, &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: 0,
		Limit:    0,
	})
	if err != nil {
		t.Fatalf("failed to get piece tasks from dfdaemon %s, err: %v", server, err)
	}
	wantPiecePacket = testServers.daemonServerImpls[0].piecePacket[testTaskID]
	if !cmp.Equal(piecePacket, wantPiecePacket, cmpopts.IgnoreUnexported(base.PiecePacket{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
		t.Fatalf("piece packet is not same with expected, expected piece tasks: %v, actual: %v", wantPiecePacket, piecePacket)
	}
}

type server struct {
	addr string
	ip   string
	port int32
}

func (s *server) String() string {
	return s.addr
}

func resolveServer(addr string) *server {
	host := strings.Split(addr, ":")
	ip := host[0]
	port, _ := strconv.Atoi(host[1])
	return &server{
		addr: addr,
		ip:   ip,
		port: int32(port),
	}
}
