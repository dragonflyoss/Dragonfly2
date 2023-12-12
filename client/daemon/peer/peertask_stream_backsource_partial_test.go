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
	"log"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	dfdaemonv1mocks "d7y.io/api/v2/pkg/apis/dfdaemon/v1/mocks"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	schedulerv1mocks "d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/rpc"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	clientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/clients/httpprotocol"
	sourcemocks "d7y.io/dragonfly/v2/pkg/source/mocks"
)

func setupBackSourcePartialComponents(ctrl *gomock.Controller, testBytes []byte, opt componentsOption) (
	schedulerclient.V1, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = dfdaemonv1mocks.NewMockDaemonServer(ctrl)

	var piecesMd5 []string
	pieceCount := int32(math.Ceil(float64(opt.contentLength) / float64(opt.pieceSize)))
	for i := int32(0); i < pieceCount; i++ {
		if int64(i+1)*int64(opt.pieceSize) > opt.contentLength {
			piecesMd5 = append(piecesMd5, digest.MD5FromBytes(testBytes[int(i)*int(opt.pieceSize):]))
		} else {
			piecesMd5 = append(piecesMd5, digest.MD5FromBytes(testBytes[int(i)*int(opt.pieceSize):int(i+1)*int(opt.pieceSize)]))
		}
	}
	daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
		var tasks []*commonv1.PieceInfo
		// only return first piece
		if request.StartNum == 0 {
			tasks = append(tasks,
				&commonv1.PieceInfo{
					PieceNum:    int32(request.StartNum),
					RangeStart:  uint64(0),
					RangeSize:   opt.pieceSize,
					PieceMd5:    digest.MD5FromBytes(testBytes[0:opt.pieceSize]),
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		return &commonv1.PiecePacket{
			PieceMd5Sign:  digest.SHA256FromStrings(piecesMd5...),
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    tasks,
			ContentLength: opt.contentLength,
			TotalPiece:    pieceCount,
		}, nil
	})
	daemon.EXPECT().SyncPieceTasks(gomock.Any()).AnyTimes().DoAndReturn(func(s dfdaemonv1.Daemon_SyncPieceTasksServer) error {
		request, err := s.Recv()
		if err != nil {
			return err
		}
		var tasks []*commonv1.PieceInfo
		// only return first piece
		if request.StartNum == 0 {
			tasks = append(tasks,
				&commonv1.PieceInfo{
					PieceNum:    int32(request.StartNum),
					RangeStart:  uint64(0),
					RangeSize:   opt.pieceSize,
					PieceMd5:    digest.MD5FromBytes(testBytes[0:opt.pieceSize]),
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		pp := &commonv1.PiecePacket{
			PieceMd5Sign:  digest.SHA256FromStrings(piecesMd5...),
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    tasks,
			ContentLength: opt.contentLength,
			TotalPiece:    pieceCount,
		}
		if err = s.Send(pp); err != nil {
			return err
		}
		for {
			_, err = s.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})
	go func(daemon *dfdaemonv1mocks.MockDaemonServer, ln net.Listener) {
		hs := health.NewServer()
		if err := daemonserver.New(daemon, hs).Serve(ln); err != nil {
			log.Fatal(err)
		}
	}(daemon, ln)
	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := schedulerv1mocks.NewMockScheduler_ReportPieceResultClient(ctrl)
	var (
		wg             = sync.WaitGroup{}
		backSourceSent = atomic.Bool{}
	)
	wg.Add(1)

	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *schedulerv1.PieceResult) error {
			if pr.PieceInfo.PieceNum == 0 && pr.Success {
				if !backSourceSent.Load() {
					wg.Done()
					backSourceSent.Store(true)
				}
			}
			return nil
		})
	var (
		delayCount      int
		schedPeerPacket bool
	)
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*schedulerv1.PeerPacket, error) {
			if len(opt.peerPacketDelay) > delayCount {
				if delay := opt.peerPacketDelay[delayCount]; delay > 0 {
					time.Sleep(delay)
				}
				delayCount++
			}
			if schedPeerPacket {
				// send back source after piece 0 is done
				wg.Wait()
				return nil, dferrors.New(commonv1.Code_SchedNeedBackSource, "")
			}
			schedPeerPacket = true
			return &schedulerv1.PeerPacket{
				Code:   commonv1.Code_Success,
				TaskId: opt.taskID,
				SrcPid: "127.0.0.1",
				MainPeer: &schedulerv1.PeerPacket_DestPeer{
					Ip:      "127.0.0.1",
					RpcPort: port,
					PeerId:  "peer-x",
				},
				CandidatePeers: nil,
			}, nil
		})
	pps.EXPECT().CloseSend().AnyTimes()
	sched := clientmocks.NewMockV1(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
			return &schedulerv1.RegisterResult{
				TaskId:      opt.taskID,
				SizeScope:   commonv1.SizeScope_NORMAL,
				DirectPiece: nil,
			}, nil
		})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {
			return pps, nil
		})
	sched.EXPECT().ReportPeerResult(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, pr *schedulerv1.PeerResult, opts ...grpc.CallOption) error {
			return nil
		})
	tempDir, _ := os.MkdirTemp("", "d7y-test-*")
	storageManager, _ := storage.NewStorageManager(
		config.SimpleLocalTaskStoreStrategy,
		&config.StorageOption{
			DataPath: tempDir,
			TaskExpireTime: util.Duration{
				Duration: -1 * time.Second,
			},
		}, func(request storage.CommonTaskRequest) {}, os.FileMode(0700))
	return sched, storageManager
}

// TestStreamPeerTask_BackSource_Partial_WithContentLength tests that get piece from other peers first, then scheduler says back source
func TestStreamPeerTask_BackSource_Partial_WithContentLength(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-back-source-partial-0"
		taskID = "task-back-source-partial-0"

		url = "http://localhost/test/data"
	)
	schedulerClient, storageManager := setupBackSourcePartialComponents(
		ctrl, testBytes,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
			content:            testBytes,
		})
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := io.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	sourceClient := sourcemocks.NewMockResourceClient(ctrl)
	source.UnRegister("http")
	require.Nil(t, source.Register("http", sourceClient, httpprotocol.Adapter))
	defer source.UnRegister("http")
	sourceClient.EXPECT().Download(gomock.Any()).DoAndReturn(
		func(request *source.Request) (*source.Response, error) {
			response := source.NewResponse(io.NopCloser(bytes.NewBuffer(testBytes)))
			response.ContentLength = int64(len(testBytes))
			return response, nil
		})

	pm := &pieceManager{
		calculateDigest: true,
		pieceDownloader: downloader,
		computePieceSize: func(contentLength int64) uint32 {
			return uint32(pieceSize)
		},
	}
	ptm := &peerTaskManager{
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		trafficShaper:    NewTrafficShaper("plain", 0, nil),
		TaskManagerOption: TaskManagerOption{
			SchedulerClient: schedulerClient,
			TaskOption: TaskOption{
				CalculateDigest: true,
				PeerHost: &schedulerv1.PeerHost{
					Ip: "127.0.0.1",
				},
				PieceManager:   pm,
				StorageManager: storageManager,
				SchedulerOption: config.SchedulerOption{
					ScheduleTimeout: util.Duration{Duration: 10 * time.Minute},
				},
				GRPCDialTimeout: time.Second,
				GRPCCredentials: insecure.NewCredentials(),
			},
		},
	}
	req := &schedulerv1.PeerTaskRequest{
		Url: url,
		UrlMeta: &commonv1.UrlMeta{
			Tag: "d7y-test",
		},
		PeerId:   peerID,
		PeerHost: &schedulerv1.PeerHost{},
	}
	ctx := context.Background()
	pt, err := ptm.newStreamTask(ctx, req, nil)
	assert.Nil(err, "new stream peer task")

	rc, _, err := pt.Start(ctx)
	assert.Nil(err, "start stream peer task")

	outputBytes, err := io.ReadAll(rc)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
