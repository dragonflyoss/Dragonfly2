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

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
	"google.golang.org/grpc"
)

type mockSchedulerClient struct {
	RegisterPeerTaskFunc func(ctx context.Context, in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PiecePackage, error)
	PullPieceTasksFunc   func(ctx context.Context, opts ...grpc.CallOption) (scheduler.Scheduler_PullPieceTasksClient, error)
	ReportPeerResultFunc func(ctx context.Context, in *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)
	LeaveTaskFunc        func(ctx context.Context, in *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
}

func (c *mockSchedulerClient) RegisterPeerTask(ctx context.Context,
	in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PiecePackage, error) {
	return c.RegisterPeerTaskFunc(ctx, in, opts...)
}

func (c *mockSchedulerClient) PullPieceTasks(ctx context.Context,
	opts ...grpc.CallOption) (scheduler.Scheduler_PullPieceTasksClient, error) {
	return c.PullPieceTasksFunc(ctx, opts...)
}

func (c *mockSchedulerClient) ReportPeerResult(ctx context.Context,
	in *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return c.ReportPeerResultFunc(ctx, in, opts...)
}

func (c *mockSchedulerClient) LeaveTask(ctx context.Context,
	in *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error) {
	return c.LeaveTaskFunc(ctx, in, opts...)
}

type mockPullPieceTasksClient struct {
	grpc.ClientStream
	SendFunc func(result *scheduler.PieceResult) error
	RecvFunc func() (*scheduler.PiecePackage, error)
}

func (p *mockPullPieceTasksClient) Send(result *scheduler.PieceResult) error {
	return p.SendFunc(result)
}

func (p *mockPullPieceTasksClient) Recv() (*scheduler.PiecePackage, error) {
	return p.RecvFunc()
}

func (p *mockPullPieceTasksClient) CloseSend() error {
	return nil
}

type mockPieceDownloader struct {
	DownloadPieceFunc func(task *DownloadPieceRequest) (io.ReadCloser, error)
}

func (d *mockPieceDownloader) DownloadPiece(task *DownloadPieceRequest) (io.ReadCloser, error) {
	return d.DownloadPieceFunc(task)
}

func TestPeerTaskManager_StartFilePeerTask(t *testing.T) {
	storage, _ := NewStorageManager("testdata", WithStorageOption(&StorageOption{
		BaseDir:    "testdata",
		GCInterval: -1 * time.Second,
	}))
	defer storage.(GC).TryGC()

	testBytes, err := ioutil.ReadFile(testFile)
	if err != nil {
		t.Fatalf("load test file failed: %s", err)
	}

	var (
		piecePackagePieceCount = 4
		pieceSize              = 1024

		mockPieceSched    = 1 // RegisterPeerTask returned the first piece
		mockContentLength = len(testBytes)
		mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"

		output = "testdata/test.output"
	)
	defer os.Remove(output)
	ptc := &mockPullPieceTasksClient{
		SendFunc: func(result *scheduler.PieceResult) error {
			return nil
		},
		RecvFunc: func() (*scheduler.PiecePackage, error) {
			var pieceTasks []*scheduler.PiecePackage_PieceTask
			for i := 0; i < piecePackagePieceCount; i++ {
				if mockPieceSched >= mockPieceCount {
					break
				}
				end := (mockPieceSched+1)*pieceSize - 1
				if end > mockContentLength {
					end = mockContentLength - 1
				}
				pieceTasks = append(pieceTasks,
					&scheduler.PiecePackage_PieceTask{
						PieceNum:    int32(mockPieceSched),
						PieceRange:  fmt.Sprintf("bytes=%d-%d", mockPieceSched*pieceSize, end),
						PieceMd5:    "",
						SrcPid:      "",
						DstPid:      "",
						DstAddr:     "",
						PieceOffset: uint64(mockPieceSched * pieceSize),
						PieceStyle:  0,
					})
				mockPieceSched++
			}
			pp := &scheduler.PiecePackage{
				State:         nil,
				TaskId:        "",
				PieceTasks:    pieceTasks,
				Last:          false,
				ContentLength: uint64(mockContentLength),
			}
			if mockPieceSched >= mockPieceCount {
				pp.Last = true
			}
			return pp, nil
		},
	}
	sched := &mockSchedulerClient{
		RegisterPeerTaskFunc: func(ctx context.Context,
			in *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PiecePackage, error) {
			return &scheduler.PiecePackage{
				State: &base.ResponseState{
					Success: true,
					Code:    base.Code_SUCCESS,
					Msg:     "progress by mockSchedulerClient",
				},
				TaskId: taskID,
				PieceTasks: []*scheduler.PiecePackage_PieceTask{
					// first piece task
					{
						PieceNum:    0,
						PieceRange:  fmt.Sprintf("bytes=0-%d", pieceSize-1),
						PieceMd5:    "",
						SrcPid:      "",
						DstPid:      "",
						DstAddr:     "",
						PieceOffset: 0,
						PieceStyle:  0,
					},
				},
				Last:          false,
				ContentLength: uint64(mockContentLength),
			}, nil
		},
		PullPieceTasksFunc: func(ctx context.Context,
			opts ...grpc.CallOption) (scheduler.Scheduler_PullPieceTasksClient, error) {
			return ptc, nil
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
			storageManager: storage,
			pieceDownloader: &mockPieceDownloader{
				DownloadPieceFunc: func(task *DownloadPieceRequest) (io.ReadCloser, error) {
					r := util.MustParseRange(task.PieceRange, int64(mockContentLength))
					return ioutil.NopCloser(bytes.NewBuffer(testBytes[r.Start : r.Start+int64(pieceSize)])), nil
				},
			},
		},
		storageManager: storage,
		scheduler:      sched,
	}
	progress, err := ptm.StartFilePeerTask(context.Background(), &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      "http://localhost/test/data",
			Filter:   "",
			BizId:    "df-test",
			UrlMata:  nil,
			Pid:      peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	})
	if err != nil {
		t.Fatalf("start file peer task failed: %s", err)
	}

	for p := range progress {
		t.Logf("progress %d/%d", p.CompletedLength, p.ContentLength)
		if p.Done {
			t.Logf("progress done")
		}
	}

	outputBytes, err := ioutil.ReadFile(output)
	if err != nil {
		t.Fatalf("load output file failed: %s", err)
	}
	if bytes.Compare(outputBytes, testBytes) != 0 {
		t.Errorf("output and desired output not match")
	}
}
