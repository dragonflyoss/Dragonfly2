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
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/gc"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/test"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

type mockSchedulerClient struct {
	RegisterPeerTaskFunc  func(ctx context.Context, in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PeerPacket, error)
	ReportPieceResultFunc func(ctx context.Context, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error)
	ReportPeerResultFunc  func(ctx context.Context, in *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)
	LeaveTaskFunc         func(ctx context.Context, in *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
}

func (c *mockSchedulerClient) RegisterPeerTask(ctx context.Context, in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PeerPacket, error) {
	return c.RegisterPeerTaskFunc(ctx, in, opts...)
}

func (c *mockSchedulerClient) ReportPieceResult(ctx context.Context, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error) {
	return c.ReportPieceResultFunc(ctx, opts...)
}

func (c *mockSchedulerClient) ReportPeerResult(ctx context.Context,
	in *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return c.ReportPeerResultFunc(ctx, in, opts...)
}

func (c *mockSchedulerClient) LeaveTask(ctx context.Context,
	in *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return c.LeaveTaskFunc(ctx, in, opts...)
}

type mockReportPieceResultClient struct {
	grpc.ClientStream
	SendFunc func(*scheduler.PieceResult) error
	RecvFunc func() (*scheduler.PeerPacket, error)
}

func (p *mockReportPieceResultClient) Send(result *scheduler.PieceResult) error {
	return p.SendFunc(result)
}

func (p *mockReportPieceResultClient) Recv() (*scheduler.PeerPacket, error) {
	return p.RecvFunc()
}

func (p *mockReportPieceResultClient) CloseSend() error {
	return nil
}

type mockPieceDownloader struct {
	DownloadPieceFunc func(task *DownloadPieceRequest) (io.ReadCloser, error)
}

func (d *mockPieceDownloader) DownloadPiece(task *DownloadPieceRequest) (io.ReadCloser, error) {
	return d.DownloadPieceFunc(task)
}

type mockDaemonClient struct {
	DownloadFunc      func(ctx context.Context, in *dfdaemon.DownRequest, opts ...grpc.CallOption) (dfdaemon.Daemon_DownloadClient, error)
	GetPieceTasksFunc func(ctx context.Context, in *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error)
	CheckHealthFunc   func(ctx context.Context, in *base.EmptyRequest, opts ...grpc.CallOption) (*base.ResponseState, error)
}

func (m mockDaemonClient) Download(ctx context.Context, in *dfdaemon.DownRequest, opts ...grpc.CallOption) (dfdaemon.Daemon_DownloadClient, error) {
	return m.DownloadFunc(ctx, in, opts...)
}

func (m mockDaemonClient) GetPieceTasks(ctx context.Context, in *base.PieceTaskRequest, opts ...grpc.CallOption) (*base.PiecePacket, error) {
	return m.GetPieceTasksFunc(ctx, in, opts...)
}

func (m mockDaemonClient) CheckHealth(ctx context.Context, in *base.EmptyRequest, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return m.CheckHealthFunc(ctx, in, opts...)
}

type mockDaemonServer struct {
	dfdaemon.UnimplementedDaemonServer
	DownloadFunc      func(request *dfdaemon.DownRequest, server dfdaemon.Daemon_DownloadServer) error
	GetPieceTasksFunc func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error)
	CheckHealthFunc   func(ctx context.Context, request *base.EmptyRequest) (*base.ResponseState, error)

	mockContentLength int64
}

func (m mockDaemonServer) Download(request *dfdaemon.DownRequest, server dfdaemon.Daemon_DownloadServer) error {
	return m.DownloadFunc(request, server)
}

func (m mockDaemonServer) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return m.GetPieceTasksFunc(ctx, request)
}

func (m mockDaemonServer) CheckHealth(ctx context.Context, request *base.EmptyRequest) (*base.ResponseState, error) {
	return m.CheckHealthFunc(ctx, request)
}

func setupDaemonServer(contentLength int64, pieceSize int32) (port int32) {
	port = int32(freeport.GetPort())
	s := grpc.NewServer()
	dfdaemon.RegisterDaemonServer(s, mockDaemonServer{
		DownloadFunc: func(request *dfdaemon.DownRequest, server dfdaemon.Daemon_DownloadServer) error {
			panic("not implement")
		},
		GetPieceTasksFunc: func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
			var tasks []*base.PieceTask
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
					&base.PieceTask{
						PieceNum:    request.StartNum + i,
						RangeStart:  uint64(start),
						RangeSize:   size,
						PieceMd5:    "",
						SrcPid:      "",
						DstPid:      "",
						DstAddr:     "",
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
				TaskId:     request.TaskId,
				PieceTasks: tasks,
			}, nil
		},
		CheckHealthFunc: func(ctx context.Context, request *base.EmptyRequest) (*base.ResponseState, error) {
			return &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "",
			}, nil
		},
	})
	l, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	go s.Serve(l)
	return port
}

func TestPeerTaskManager_StartFilePeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	storageManager, _ := storage.NewStorageManager(storage.SimpleLocalTaskStoreDriver, &storage.Option{
		DataPath:       test.DataDir,
		TaskExpireTime: -1 * time.Second,
	})
	defer storageManager.(gc.GC).TryGC()

	testBytes, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = 4
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"

		output = "../test/testdata/test.output"
	)
	defer os.Remove(output)
	port := setupDaemonServer(int64(mockContentLength), int32(pieceSize))
	time.Sleep(100 * time.Millisecond)
	rprc := &mockReportPieceResultClient{
		SendFunc: func(result *scheduler.PieceResult) error {
			return nil
		},
		RecvFunc: func() (*scheduler.PeerPacket, error) {
			time.Sleep(10 * time.Second)
			return &scheduler.PeerPacket{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "progress by mockReportPieceResultClient",
				},
				TaskId:        taskID,
				SrcPid:        "127.0.0.1",
				ParallelCount: int32(pieceParallelCount),
				MainPeer: &scheduler.PeerHost{
					Uuid:           "",
					Ip:             "127.0.0.1",
					Port:           port,
					HostName:       "",
					SecurityDomain: "",
					Location:       "",
					Idc:            "",
					Switch:         "",
				},
				StealPeers:    nil,
				Done:          true,
				ContentLength: int64(mockContentLength),
			}, nil
		},
	}
	sched := &mockSchedulerClient{
		RegisterPeerTaskFunc: func(ctx context.Context,
			in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PeerPacket, error) {
			return &scheduler.PeerPacket{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "progress by mockSchedulerClient",
				},
				TaskId:        taskID,
				SrcPid:        "127.0.0.1",
				ParallelCount: 4,
				MainPeer: &scheduler.PeerHost{
					Uuid:           "",
					Ip:             "127.0.0.1",
					Port:           port,
					HostName:       "",
					SecurityDomain: "",
					Location:       "",
					Idc:            "",
					Switch:         "",
				},
				StealPeers:    nil,
				Done:          true,
				ContentLength: int64(mockContentLength),
			}, nil
		},
		ReportPieceResultFunc: func(ctx context.Context, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error) {
			return rprc, nil
		},
		ReportPeerResultFunc: func(ctx context.Context,
			in *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
			return &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "progress by mockSchedulerClient",
			}, nil
		},
		LeaveTaskFunc: nil,
	}

	ptm := &peerTaskManager{
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager: storageManager,
			pieceDownloader: &mockPieceDownloader{
				DownloadPieceFunc: func(task *DownloadPieceRequest) (io.ReadCloser, error) {
					return ioutil.NopCloser(bytes.NewBuffer(testBytes[task.RangeStart : task.RangeStart+uint64(task.RangeSize)])), nil
				},
			},
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
