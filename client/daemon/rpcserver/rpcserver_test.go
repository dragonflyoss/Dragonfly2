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

package rpcserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	mock_peer "d7y.io/dragonfly/v2/client/daemon/test/mock/peer"
	mock_storage "d7y.io/dragonfly/v2/client/daemon/test/mock/storage"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDownloadManager_ServeDownload(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPeerTaskManager := mock_peer.NewMockTaskManager(ctrl)
	mockPeerTaskManager.EXPECT().StartFilePeerTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *peer.FilePeerTaskRequest) (chan *peer.FilePeerTaskProgress, bool, error) {
			ch := make(chan *peer.FilePeerTaskProgress)
			go func() {
				for i := 0; i <= 100; i++ {
					ch <- &peer.FilePeerTaskProgress{
						State: &peer.ProgressState{
							Success: true,
						},
						TaskID:          "",
						PeerID:          "",
						ContentLength:   100,
						CompletedLength: int64(i),
						PeerTaskDone:    i == 100,
						DoneCallback:    func() {},
					}
				}
				close(ch)
			}()
			return ch, false, nil
		})
	m := &server{
		KeepAlive:       clientutil.NewKeepAlive("test"),
		peerHost:        &scheduler.PeerHost{},
		peerTaskManager: mockPeerTaskManager,
	}
	m.downloadServer = dfdaemonserver.New(m)
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(err, "get free port should be ok")
	go func() {
		m.ServeDownload(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	client, err := dfclient.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", port),
		},
	})
	assert.Nil(err, "grpc dial should be ok")
	request := &dfdaemongrpc.DownRequest{
		Url:    "http://localhost/test",
		Output: "./testdata/file1",
		UrlMeta: &base.UrlMeta{
			Tag: "unit test",
		},
	}
	down, err := client.Download(context.Background(), request)
	assert.Nil(err, "client download grpc call should be ok")

	var (
		lastResult *dfdaemongrpc.DownResult
		curResult  *dfdaemongrpc.DownResult
	)
	for {
		curResult, err = down.Recv()
		if err == io.EOF {
			break
		}
		assert.Nil(err)
		lastResult = curResult
	}
	assert.NotNil(lastResult)
	assert.True(lastResult.Done)
}

func TestDownloadManager_ServePeer(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var maxPieceNum int32 = 10
	mockStorageManger := mock_storage.NewMockManager(ctrl)
	mockStorageManger.EXPECT().GetPieces(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var (
			pieces    []*base.PieceInfo
			pieceSize = int32(1024)
		)
		for i := req.StartNum; i < req.Limit+req.StartNum && i < maxPieceNum; i++ {
			pieces = append(pieces, &base.PieceInfo{
				PieceNum:    i,
				RangeStart:  uint64(i * pieceSize),
				RangeSize:   pieceSize,
				PieceMd5:    "",
				PieceOffset: uint64(i * pieceSize),
				PieceStyle:  base.PieceStyle_PLAIN,
			})
		}
		return &base.PiecePacket{
			TaskId:        "",
			DstPid:        "",
			DstAddr:       "",
			PieceInfos:    pieces,
			TotalPiece:    10,
			ContentLength: 10 * int64(pieceSize),
			PieceMd5Sign:  "",
		}, nil
	})
	m := &server{
		KeepAlive:      clientutil.NewKeepAlive("test"),
		peerHost:       &scheduler.PeerHost{},
		storageManager: mockStorageManger,
	}
	m.peerServer = dfdaemonserver.New(m)
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(err, "get free port should be ok")
	go func() {
		m.ServePeer(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	client, err := dfclient.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", port),
		},
	})
	assert.Nil(err, "grpc dial should be ok")

	var tests = []struct {
		request           *base.PieceTaskRequest
		responsePieceSize int
	}{
		{
			request: &base.PieceTaskRequest{
				StartNum: 0,
				Limit:    1,
			},
			// 0
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 0,
				Limit:    4,
			},
			// 0 1 2 3
			responsePieceSize: 4,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 8,
				Limit:    1,
			},
			// 8
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 8,
				Limit:    4,
			},
			// 8 9
			responsePieceSize: 2,
		},
	}

	for _, tc := range tests {
		response, err := client.GetPieceTasks(
			context.Background(),
			dfnet.NetAddr{
				Type: dfnet.TCP,
				Addr: fmt.Sprintf("127.0.0.1:%d", port),
			},
			tc.request)
		assert.Nil(err, "client get piece tasks grpc call should be ok")
		assert.Equal(tc.responsePieceSize, len(response.PieceInfos))
	}
}
