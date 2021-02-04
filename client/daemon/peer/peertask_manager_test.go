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

package peer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly/v2/client/daemon/gc"
	"github.com/dragonflyoss/Dragonfly/v2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly/v2/client/daemon/test"
	mock_daemon "github.com/dragonflyoss/Dragonfly/v2/client/daemon/test/mock/daemon"
	mock_scheduler "github.com/dragonflyoss/Dragonfly/v2/client/daemon/test/mock/scheduler"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly/v2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/base"
	daemonserver "github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/scheduler"
)

var _ daemonserver.DaemonServer = mock_daemon.NewMockDaemonServer(nil)

func TestMain(m *testing.M) {
	logger.InitDaemon()
	m.Run()
}

func setupDaemonServer(ctrl *gomock.Controller, contentLength int64, pieceSize int32) (port int32) {
	port = int32(freeport.GetPort())
	var daemon = mock_daemon.NewMockDaemonServer(ctrl)
	daemon.EXPECT().CheckHealth(gomock.Any()).DoAndReturn(func(ctx context.Context) (*base.ResponseState, error) {
		return &base.ResponseState{
			Success: true,
			Code:    base.Code_SUCCESS,
			Msg:     "",
		}, nil
	})
	daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var tasks []*base.PieceInfo
		for i := int32(0); i < request.Limit; i++ {
			start := pieceSize * (request.StartNum + i)
			if int64(start)+1 > contentLength {
				break
			}
			size := pieceSize
			if int64(start+pieceSize) > contentLength {
				size = int32(contentLength) - start
			}
			tasks = append(tasks,
				&base.PieceInfo{
					PieceNum:    request.StartNum + i,
					RangeStart:  uint64(start),
					RangeSize:   size,
					PieceMd5:    "",
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		return &base.PiecePacket{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "",
			},
			TaskId:        request.TaskId,
			PieceInfos:    tasks,
			ContentLength: contentLength,
		}, nil
	})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})
	go rpc.NewServer(daemon).Serve(ln)
	return port
}

func TestPeerTaskManager_StartFilePeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	storageManager, _ := storage.NewStorageManager(storage.SimpleLocalTaskStoreStrategy, &storage.Option{
		DataPath:       test.DataDir,
		TaskExpireTime: -1 * time.Second,
	}, func(request storage.CommonTaskRequest) {})
	defer storageManager.(gc.GC).TryGC()

	testBytes, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"

		output = "../test/testdata/test.output"
	)
	defer os.Remove(output)
	port := setupDaemonServer(ctrl, int64(mockContentLength), int32(pieceSize))
	time.Sleep(100 * time.Millisecond)
	sched := mock_scheduler.NewMockSchedulerClient(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
		return &scheduler.RegisterResult{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "",
			},
			TaskId:      taskID,
			SizeScope:   base.SizeScope_NORMAL,
			DirectPiece: nil,
		}, nil
	})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error) {
		resultCh := make(chan *scheduler.PieceResult)
		peerPacketCh := make(chan *scheduler.PeerPacket)
		go func() {
			for {
				select {
				case <-resultCh:
				}
			}
		}()
		go func() {
			peerPacketCh <- &scheduler.PeerPacket{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "progress by mockSchedulerClient",
				},
				TaskId:        taskID,
				SrcPid:        "127.0.0.1",
				ParallelCount: pieceParallelCount,
				MainPeer: &scheduler.PeerPacket_DestPeer{
					Ip:      "127.0.0.1",
					RpcPort: port,
					PeerId:  "",
				},
				StealPeers: nil,
			}
		}()
		return resultCh, peerPacketCh, nil
	})
	sched.EXPECT().ReportPeerResult(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
		return &base.ResponseState{
			Success: true,
			Code:    base.Code_SUCCESS,
			Msg:     "progress by mockSchedulerClient",
		}, nil
	})

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.ReadCloser, error) {
		return ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			)), nil
	})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Uuid:           "",
			Ip:             "127.0.0.1",
			RpcPort:        0,
			DownPort:       0,
			HostName:       "",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:    "",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager: storageManager,
		scheduler:      sched,
	}
	progress, err := ptm.StartFilePeerTask(context.Background(), &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      "http://localhost/test/data",
			Filter:   "",
			BizId:    "d7s-test",
			UrlMata:  nil,
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	})
	assert.Nil(err, "start file peer task")

	var p *PeerTaskProgress
	for p = range progress {
		assert.True(p.State.Success)
	}
	assert.NotNil(p)
	assert.True(p.Done)

	outputBytes, err := ioutil.ReadFile(output)
	assert.Nil(err, "load output file")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartStreamPeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	storageManager, _ := storage.NewStorageManager(storage.SimpleLocalTaskStoreStrategy, &storage.Option{
		DataPath:       test.DataDir,
		TaskExpireTime: -1 * time.Second,
	}, func(request storage.CommonTaskRequest) {})
	defer storageManager.(gc.GC).TryGC()

	testBytes, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"
	)
	port := setupDaemonServer(ctrl, int64(mockContentLength), int32(pieceSize))
	time.Sleep(100 * time.Millisecond)
	sched := mock_scheduler.NewMockSchedulerClient(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
		return &scheduler.RegisterResult{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "",
			},
			TaskId:      taskID,
			SizeScope:   base.SizeScope_NORMAL,
			DirectPiece: nil,
		}, nil
	})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error) {
		resultCh := make(chan *scheduler.PieceResult)
		peerPacketCh := make(chan *scheduler.PeerPacket)
		go func() {
			for {
				select {
				case <-resultCh:
				}
			}
		}()
		go func() {
			peerPacketCh <- &scheduler.PeerPacket{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "progress by mockSchedulerClient",
				},
				TaskId:        taskID,
				SrcPid:        "127.0.0.1",
				ParallelCount: pieceParallelCount,
				MainPeer: &scheduler.PeerPacket_DestPeer{
					Ip:      "127.0.0.1",
					RpcPort: port,
					PeerId:  "",
				},
				StealPeers: nil,
			}
		}()
		return resultCh, peerPacketCh, nil
	})
	sched.EXPECT().ReportPeerResult(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
		return &base.ResponseState{
			Success: true,
			Code:    base.Code_SUCCESS,
			Msg:     "progress by mockSchedulerClient",
		}, nil
	})

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.ReadCloser, error) {
		return ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			)), nil
	})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Uuid:           "",
			Ip:             "127.0.0.1",
			RpcPort:        0,
			DownPort:       0,
			HostName:       "",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			NetTopology:    "",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager: storageManager,
		scheduler:      sched,
	}
	r, _, err := ptm.StartStreamPeerTask(context.Background(), &scheduler.PeerTaskRequest{
		Url:      "http://localhost/test/data",
		Filter:   "",
		BizId:    "d7s-test",
		UrlMata:  nil,
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	})
	assert.Nil(err, "start stream peer task")

	outputBytes, err := ioutil.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
