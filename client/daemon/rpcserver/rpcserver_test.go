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
	"sync"
	"testing"

	"github.com/distribution/distribution/v3/uuid"
	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	mock_peer "d7y.io/dragonfly/v2/client/daemon/test/mock/peer"
	mock_storage "d7y.io/dragonfly/v2/client/daemon/test/mock/storage"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDownloadManager_ServeDownload(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPeerTaskManager := mock_peer.NewMockTaskManager(ctrl)
	mockPeerTaskManager.EXPECT().StartFileTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *peer.FileTaskRequest) (chan *peer.FileTaskProgress, bool, error) {
			ch := make(chan *peer.FileTaskProgress)
			go func() {
				for i := 0; i <= 100; i++ {
					ch <- &peer.FileTaskProgress{
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
	_, client := setupPeerServerAndClient(t, m, assert, m.ServeDownload)
	request := &dfdaemongrpc.DownRequest{
		Uuid:              uuid.Generate().String(),
		Url:               "http://localhost/test",
		Output:            "./testdata/file1",
		DisableBackSource: false,
		UrlMeta: &base.UrlMeta{
			Tag: "unit test",
		},
		Pattern:    "p2p",
		Callsystem: "",
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

	var maxPieceNum uint32 = 10
	mockStorageManger := mock_storage.NewMockManager(ctrl)
	mockStorageManger.EXPECT().GetPieces(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var (
			pieces    []*base.PieceInfo
			pieceSize = uint32(1024)
		)
		for i := req.StartNum; i < req.Limit+req.StartNum && i < maxPieceNum; i++ {
			pieces = append(pieces, &base.PieceInfo{
				PieceNum:    int32(i),
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
	s := &server{
		KeepAlive:      clientutil.NewKeepAlive("test"),
		peerHost:       &scheduler.PeerHost{},
		storageManager: mockStorageManger,
	}
	s.peerServer = dfdaemonserver.New(s)
	port, client := setupPeerServerAndClient(t, s, assert, s.ServePeer)
	defer s.peerServer.GracefulStop()

	var tests = []struct {
		request           *base.PieceTaskRequest
		responsePieceSize int
	}{
		{
			request: &base.PieceTaskRequest{
				TaskId:   idgen.TaskID("http://www.test.com", &base.UrlMeta{}),
				SrcPid:   idgen.PeerID(iputils.IPv4),
				DstPid:   idgen.PeerID(iputils.IPv4),
				StartNum: 0,
				Limit:    1,
			},
			// 0
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				TaskId:   idgen.TaskID("http://www.test.com", &base.UrlMeta{}),
				SrcPid:   idgen.PeerID(iputils.IPv4),
				DstPid:   idgen.PeerID(iputils.IPv4),
				StartNum: 0,
				Limit:    4,
			},
			// 0 1 2 3
			responsePieceSize: 4,
		},
		{
			request: &base.PieceTaskRequest{
				TaskId:   idgen.TaskID("http://www.test.com", &base.UrlMeta{}),
				SrcPid:   idgen.PeerID(iputils.IPv4),
				DstPid:   idgen.PeerID(iputils.IPv4),
				StartNum: 8,
				Limit:    1,
			},
			// 8
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				TaskId:   idgen.TaskID("http://www.test.com", &base.UrlMeta{}),
				SrcPid:   idgen.PeerID(iputils.IPv4),
				DstPid:   idgen.PeerID(iputils.IPv4),
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

func TestDownloadManager_SyncPieceTasks(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		pieceSize = uint32(1024)
	)

	type pieceRange struct {
		start int
		end   int
	}
	var tests = []struct {
		name            string
		existTaskID     string // test for non-exists task
		existPieces     []pieceRange
		requestPieces   []int32
		limit           uint32
		totalPieces     uint32
		followingPieces []pieceRange
		success         bool
		verify          func(t *testing.T, assert *testifyassert.Assertions)
	}{
		{
			name: "already exists in storage",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   10,
				},
			},
			totalPieces: 11,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "already exists in storage - large",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   1000,
				},
			},
			totalPieces: 1001,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "partial exists in storage",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   10,
				},
			},
			followingPieces: []pieceRange{
				{
					start: 11,
					end:   20,
				},
			},
			totalPieces: 21,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "partial exists in storage - large",
			existPieces: []pieceRange{
				{
					start: 0,
					end:   1000,
				},
			},
			followingPieces: []pieceRange{
				{
					start: 1001,
					end:   2000,
				},
			},
			totalPieces:   2001,
			requestPieces: []int32{1000, 1010, 1020, 1040},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "not exists in storage",
			followingPieces: []pieceRange{
				{
					start: 0,
					end:   20,
				},
			},
			totalPieces: 21,
			success:     true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
		{
			name: "not exists in storage - large",
			followingPieces: []pieceRange{
				{
					start: 0,
					end:   2000,
				},
			},
			totalPieces:   2001,
			requestPieces: []int32{1000, 1010, 1020, 1040},
			success:       true,
			verify: func(t *testing.T, assert *testifyassert.Assertions) {
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, delay := range []bool{false, true} {
				delay := delay
				mockStorageManger := mock_storage.NewMockManager(ctrl)

				if tc.limit == 0 {
					tc.limit = 1024
				}

				var (
					totalPieces []*base.PieceInfo
					lock        sync.Mutex
				)

				var addedPieces = make(map[uint32]*base.PieceInfo)
				for _, p := range tc.existPieces {
					if p.end == 0 {
						p.end = p.start
					}
					for i := p.start; i <= p.end; i++ {
						if _, ok := addedPieces[uint32(i)]; ok {
							continue
						}
						piece := &base.PieceInfo{
							PieceNum:    int32(i),
							RangeStart:  uint64(i) * uint64(pieceSize),
							RangeSize:   pieceSize,
							PieceOffset: uint64(i) * uint64(pieceSize),
							PieceStyle:  base.PieceStyle_PLAIN,
						}
						totalPieces = append(totalPieces, piece)
						addedPieces[uint32(i)] = piece
					}
				}

				mockStorageManger.EXPECT().GetPieces(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
						var pieces []*base.PieceInfo
						lock.Lock()
						for i := req.StartNum; i < tc.totalPieces; i++ {
							if piece, ok := addedPieces[i]; ok {
								if piece.PieceNum >= int32(req.StartNum) && len(pieces) < int(req.Limit) {
									pieces = append(pieces, piece)
								}
							}
						}
						lock.Unlock()
						return &base.PiecePacket{
							TaskId:        req.TaskId,
							DstPid:        req.DstPid,
							DstAddr:       "",
							PieceInfos:    pieces,
							TotalPiece:    int32(tc.totalPieces),
							ContentLength: int64(tc.totalPieces) * int64(pieceSize),
							PieceMd5Sign:  "",
						}, nil
					})
				mockTaskManager := mock_peer.NewMockTaskManager(ctrl)
				mockTaskManager.EXPECT().Subscribe(gomock.Any()).AnyTimes().DoAndReturn(
					func(request *base.PieceTaskRequest) (*peer.SubscribeResult, bool) {
						ch := make(chan *peer.PieceInfo)
						success := make(chan struct{})
						fail := make(chan struct{})

						go func(followingPieces []pieceRange) {
							for i, p := range followingPieces {
								if p.end == 0 {
									p.end = p.start
								}
								for j := p.start; j <= p.end; j++ {
									lock.Lock()
									if _, ok := addedPieces[uint32(j)]; ok {
										continue
									}
									piece := &base.PieceInfo{
										PieceNum:    int32(j),
										RangeStart:  uint64(j) * uint64(pieceSize),
										RangeSize:   pieceSize,
										PieceOffset: uint64(j) * uint64(pieceSize),
										PieceStyle:  base.PieceStyle_PLAIN,
									}
									totalPieces = append(totalPieces, piece)
									addedPieces[uint32(j)] = piece
									lock.Unlock()

									var finished bool
									if i == len(followingPieces)-1 && j == p.end {
										finished = true
									}
									if !delay {
										ch <- &peer.PieceInfo{
											Num:      int32(j),
											Finished: finished,
										}
									}
								}
							}
							close(success)
						}(tc.followingPieces)

						return &peer.SubscribeResult{
							Storage:          mockStorageManger,
							PieceInfoChannel: ch,
							Success:          success,
							Fail:             fail,
						}, true
					})

				s := &server{
					KeepAlive:       clientutil.NewKeepAlive("test"),
					peerHost:        &scheduler.PeerHost{},
					storageManager:  mockStorageManger,
					peerTaskManager: mockTaskManager,
				}

				port, client := setupPeerServerAndClient(t, s, assert, s.ServePeer)
				defer s.peerServer.GracefulStop()

				syncClient, err := client.SyncPieceTasks(
					context.Background(),
					dfnet.NetAddr{
						Type: dfnet.TCP,
						Addr: fmt.Sprintf("127.0.0.1:%d", port),
					},
					&base.PieceTaskRequest{
						TaskId:   tc.name,
						SrcPid:   idgen.PeerID(iputils.IPv4),
						DstPid:   idgen.PeerID(iputils.IPv4),
						StartNum: 0,
						Limit:    tc.limit,
					})
				assert.Nil(err, "client sync piece tasks grpc call should be ok")

				var (
					total       = make(map[int32]bool)
					maxNum      int32
					requestSent = make(chan bool)
				)
				if len(tc.requestPieces) == 0 {
					close(requestSent)
				} else {
					go func() {
						for _, n := range tc.requestPieces {
							request := &base.PieceTaskRequest{
								TaskId:   tc.name,
								SrcPid:   idgen.PeerID(iputils.IPv4),
								DstPid:   idgen.PeerID(iputils.IPv4),
								StartNum: uint32(n),
								Limit:    tc.limit,
							}
							assert.Nil(syncClient.Send(request))
						}
						close(requestSent)
					}()
				}
				for {
					p, err := syncClient.Recv()
					if err == io.EOF {
						break
					}
					for _, info := range p.PieceInfos {
						total[info.PieceNum] = true
						if info.PieceNum >= maxNum {
							maxNum = info.PieceNum
						}
					}
					if tc.success {
						assert.Nil(err, "receive piece tasks should be ok")
					}
					if int(p.TotalPiece) == len(total) {
						<-requestSent
						err = syncClient.CloseSend()
						assert.Nil(err)
					}
				}
				if tc.success {
					assert.Equal(int(maxNum+1), len(total))
				}
			}

		})
	}
}

func setupPeerServerAndClient(t *testing.T, srv *server, assert *testifyassert.Assertions, serveFunc func(listener net.Listener) error) (int, dfclient.DaemonClient) {
	srv.peerServer = dfdaemonserver.New(srv)
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(err, "get free port should be ok")
	go func() {
		if err := serveFunc(ln); err != nil {
			t.Error(err)
		}
	}()

	client, err := dfclient.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", port),
		},
	})
	assert.Nil(err, "grpc dial should be ok")
	return port, client
}
