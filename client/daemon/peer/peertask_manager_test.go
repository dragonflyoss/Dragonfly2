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
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	mock_daemon "d7y.io/dragonfly/v2/client/daemon/test/mock/daemon"
	mock_scheduler "d7y.io/dragonfly/v2/client/daemon/test/mock/scheduler"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

var _ daemonserver.DaemonServer = mock_daemon.NewMockDaemonServer(nil)

func setupPeerTaskManagerComponents(
	ctrl *gomock.Controller,
	taskID string,
	contentLength int64,
	pieceSize, pieceParallelCount int32) (
	schedulerclient.SchedulerClient, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. setup a mock daemon server for uploading pieces info
	var daemon = mock_daemon.NewMockDaemonServer(ctrl)
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
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    tasks,
			ContentLength: contentLength,
			TotalPiece:    int32(math.Ceil(float64(contentLength) / float64(pieceSize))),
		}, nil
	})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})
	go rpc.NewServer(daemon).Serve(ln)
	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := mock_scheduler.NewMockPeerPacketStream(ctrl)
	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *scheduler.PieceResult) error {
			return nil
		})
	var ppsent bool
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*scheduler.PeerPacket, error) {
			if !ppsent {
				ppsent = true
				return &scheduler.PeerPacket{
					Code:          dfcodes.Success,
					TaskId:        taskID,
					SrcPid:        "127.0.0.1",
					ParallelCount: pieceParallelCount,
					MainPeer: &scheduler.PeerPacket_DestPeer{
						Ip:      "127.0.0.1",
						RpcPort: port,
						PeerId:  "peer-x",
					},
					StealPeers: nil,
				}, nil
			}
			time.Sleep(time.Hour)
			return nil, nil
		})
	sched := mock_scheduler.NewMockSchedulerClient(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
			return &scheduler.RegisterResult{
				TaskId:      taskID,
				SizeScope:   base.SizeScope_NORMAL,
				DirectPiece: nil,
			}, nil
		})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (schedulerclient.PeerPacketStream, error) {
			return pps, nil
		})
	sched.EXPECT().ReportPeerResult(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) error {
			return nil
		})
	storageManager, _ := storage.NewStorageManager(
		config.SimpleLocalTaskStoreStrategy,
		&config.StorageOption{
			DataPath: test.DataDir,
			TaskExpireTime: clientutil.Duration{
				Duration: -1 * time.Second,
			},
		}, func(request storage.CommonTaskRequest) {})
	return sched, storageManager
}

func TestPeerTaskManager_StartFilePeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).Times(
		int(math.Ceil(float64(len(testBytes)) / float64(pieceSize)))).DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := ioutil.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	progress, _, err := ptm.StartFilePeerTask(context.Background(), &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      "http://localhost/test/data",
			Filter:   "",
			BizId:    "d7y-test",
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	})
	assert.Nil(err, "start file peer task")

	var p *FilePeerTaskProgress
	for p = range progress {
		assert.True(p.State.Success)
		if p.PeerTaskDone {
			p.DoneCallback()
			break
		}
	}
	assert.NotNil(p)
	assert.True(p.PeerTaskDone)

	outputBytes, err := ioutil.ReadFile(output)
	assert.Nil(err, "load output file")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartStreamPeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	sched, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := ioutil.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager:  storageManager,
		schedulerClient: sched,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}

	r, _, err := ptm.StartStreamPeerTask(context.Background(), &scheduler.PeerTaskRequest{
		Url:      "http://localhost/test/data",
		Filter:   "",
		BizId:    "d7y-test",
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	})
	assert.Nil(err, "start stream peer task")

	outputBytes, err := ioutil.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
