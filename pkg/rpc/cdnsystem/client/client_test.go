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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"net"
	"os"
	"testing"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"google.golang.org/grpc"
)

type testServer struct {
	cdnsystem.UnimplementedSeederServer
	seedPieces map[string][]*cdnsystem.PieceSeed
}

func (s *testServer) loadPieces() {
	data, err := os.ReadFile("../testdata/seed_piece_info.json")
	if err != nil {
		log.Fatalf("failed to load seed piece info: %v", err)
	}
	if err := json.Unmarshal(data, &s.seedPieces); err != nil {
		log.Fatalf("failed to load piece info: %v", err)
	}
}

func (s *testServer) ObtainSeeds(request *cdnsystem.SeedRequest, stream cdnsystem.Seeder_ObtainSeedsServer) error {
	pieceInfos, ok := s.seedPieces[request.TaskId]
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "task not found")
	}
	for _, info := range pieceInfos {
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

func (s *testServer) GetPieceTasks(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
	pieces, ok := s.seedPieces[req.TaskId]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "task not found")
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

var _ cdnsystem.SeederServer = (*testServer)(nil)

func newTestServer() *testServer {
	s := &testServer{}
	s.loadPieces()
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
		sImpl := newTestServer()
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

	var testTask = "task1"
	verifyPieces := test.serverImpls[0].seedPieces[testTask]
	{
		stream, err := cdnClient.ObtainSeeds(context.Background(), &cdnsystem.SeedRequest{
			TaskId:  testTask,
			Url:     "https://dragonfly.com",
			UrlMeta: nil,
		})
		if err != nil {
			t.Fatalf("failed to call ObtainSeeds: %v", err)
		}
		var count int32
		for {
			piece, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("failed to recv piece: %v", err)
			}
			if !cmp.Equal(piece, verifyPieces[count], cmpopts.IgnoreUnexported(cdnsystem.PieceSeed{}), cmpopts.IgnoreUnexported(base.PieceInfo{})) {
				t.Fatalf("failed to verify piece, want:%v, actual: %v", verifyPieces[count], piece)
			}
			count++
		}
	}

	{
		if _, err := cdnClient.GetPieceTasks(context.Background(), dfnet.NetAddr{Addr: test.addresses[0]}, &base.PieceTaskRequest{
			TaskId:   testTask,
			SrcPid:   "peer1",
			DstPid:   "peer2",
			StartNum: 0,
			Limit:    1,
		}); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
	}
}

func TestMigration(t *testing.T) {
	test, err := startTestServers(2)
	if err != nil {
		t.Fatalf("failed to get cdn client: %v", err)
	}
	defer test.cleanup()

	cdnClient, err := GetClientByAddrs([]dfnet.NetAddr{{Addr: test.addresses[0]}, {Addr: test.addresses[1]}})
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cdnClient.Close()

	// The first RPC should succeed.
	{
		//if _, err := cdnClient.ObtainSeeds(context.Background(), &testpb.Empty{}); err != nil {
		//	t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		//}
	}

	// Because each testServer is disposable, the second RPC should fail.
	{
		//if _, err := cdnClient.ObtainSeeds(context.Background(), &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		//	t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
		//}
	}

	// The third RPC change the Attempt in PickReq, so it should succeed.
	{
		//if _, err := cdnClient.ObtainSeeds(context.Background(), &testpb.Empty{}); err != nil {
		//	t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		//}
	}
}
