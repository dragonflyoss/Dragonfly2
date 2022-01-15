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
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
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

func (s pieceTaskServer) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
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
	servers     []*grpc.Server
	serverImpls []*pieceTaskServer
	addresses   []string
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
	for i := 0; i < daemonServerCount; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestPieceTaskServer(lis.Addr().String(), piecePackets)
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

	for i := 0; i < cdnServerCount; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := newTestPieceTaskServer(lis.Addr().String(), piecePackets)
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

func TestElasticClient(t *testing.T) {
	testServers, err := startTestPieceTaskServers(10, 10)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer testServers.cleanup()

	client, err := GetElasticClient()
	if err != nil {
		t.Fatalf("failed to get daemon client: %v", err)
	}
	defer client.Close()

	testTaskID := "aaaaaaaaaaaaaaa"
	req := &base.PieceTaskRequest{
		TaskId:   testTaskID,
		SrcPid:   "peer1",
		DstPid:   "peer2",
		StartNum: 0,
		Limit:    3,
	}

	//
	_, err = client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
		Ip:      "1.1.1.1",
		RpcPort: 80,
		PeerId:  "xxx_CDN",
	}, req)
	if err == nil || status.Code(err) != codes.NotFound {
		t.Fatalf("")
	}
	// get piece from CDN
	//piecePacket, err := client.GetPieceTasks(context.Background(), &scheduler.PeerPacket_DestPeer{
	//	Ip:      "",
	//	RpcPort: 0,
	//	PeerId:  "",
	//}, req)
	//
	//if err != nil {
	//	t.Fatalf("failed to call GetPieceTasks: %v", err)
	//}
	//if len(piecePacket.PieceInfos) != int(req.Limit) {
	//	t.Fatalf("piece count is not same with req, want: %d, actual: %d", req.Limit, len(piecePacket.PieceInfos))
	//}
	//verifyPieces := test.serverImpls[0].piecePacket[testTaskID].PieceInfos
	//var pieceIndex = 0
	//for {
	//	if !cmp.Equal(piecePacket.PieceInfos[pieceIndex], verifyPieces[pieceIndex], cmpopts.IgnoreUnexported(base.PieceInfo{})) {
	//		t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[pieceIndex], piecePacket.PieceInfos[pieceIndex])
	//	}
	//	pieceIndex++
	//	if pieceIndex == int(req.Limit) {
	//		break
	//	}
	//}
}
