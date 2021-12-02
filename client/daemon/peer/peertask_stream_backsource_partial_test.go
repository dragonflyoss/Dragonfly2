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
	"log"
	"math"
	"net"
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
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/source"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	rangers "d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

func setupBackSourcePartialComponents(ctrl *gomock.Controller, testBytes []byte, opt componentsOption) (
	schedulerclient.SchedulerClient, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = mock_daemon.NewMockDaemonServer(ctrl)

	var piecesMd5 []string
	pieceCount := int32(math.Ceil(float64(opt.contentLength) / float64(opt.pieceSize)))
	for i := int32(0); i < pieceCount; i++ {
		if int64(i+1)*int64(opt.pieceSize) > opt.contentLength {
			piecesMd5 = append(piecesMd5, digestutils.Md5Bytes(testBytes[int(i)*int(opt.pieceSize):]))
		} else {
			piecesMd5 = append(piecesMd5, digestutils.Md5Bytes(testBytes[int(i)*int(opt.pieceSize):int(i+1)*int(opt.pieceSize)]))
		}
	}
	daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var tasks []*base.PieceInfo
		// only return first piece
		if request.StartNum == 0 {
			tasks = append(tasks,
				&base.PieceInfo{
					PieceNum:    int32(request.StartNum),
					RangeStart:  uint64(0),
					RangeSize:   opt.pieceSize,
					PieceMd5:    digestutils.Md5Bytes(testBytes[0:opt.pieceSize]),
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		return &base.PiecePacket{
			PieceMd5Sign:  digestutils.Sha256(piecesMd5...),
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    tasks,
			ContentLength: opt.contentLength,
			TotalPiece:    pieceCount,
		}, nil
	})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})
	go func(daemon *mock_daemon.MockDaemonServer, ln net.Listener) {
		if err := daemonserver.New(daemon).Serve(ln); err != nil {
			log.Fatal(err)
		}
	}(daemon, ln)
	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := mock_scheduler.NewMockPeerPacketStream(ctrl)
	wg := sync.WaitGroup{}
	wg.Add(1)
	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *scheduler.PieceResult) error {
			if pr.PieceInfo.PieceNum == 0 && pr.Success {
				wg.Done()
			}
			return nil
		})
	var (
		delayCount      int
		schedPeerPacket bool
	)
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*scheduler.PeerPacket, error) {
			if len(opt.peerPacketDelay) > delayCount {
				if delay := opt.peerPacketDelay[delayCount]; delay > 0 {
					time.Sleep(delay)
				}
				delayCount++
			}
			if schedPeerPacket {
				// send back source after piece 0 is done
				wg.Wait()
				return nil, dferrors.New(base.Code_SchedNeedBackSource, "")
			}
			schedPeerPacket = true
			return &scheduler.PeerPacket{
				Code:          base.Code_Success,
				TaskId:        opt.taskID,
				SrcPid:        "127.0.0.1",
				ParallelCount: opt.pieceParallelCount,
				MainPeer: &scheduler.PeerPacket_DestPeer{
					Ip:      "127.0.0.1",
					RpcPort: port,
					PeerId:  "peer-x",
				},
				StealPeers: nil,
			}, nil
		})
	sched := mock_scheduler.NewMockSchedulerClient(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
			return &scheduler.RegisterResult{
				TaskId:      opt.taskID,
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
	tempDir, _ := ioutil.TempDir("", "d7y-test-*")
	storageManager, _ := storage.NewStorageManager(
		config.SimpleLocalTaskStoreStrategy,
		&config.StorageOption{
			DataPath: tempDir,
			TaskExpireTime: clientutil.Duration{
				Duration: -1 * time.Second,
			},
		}, func(request storage.CommonTaskRequest) {})
	return sched, storageManager
}

func TestStreamPeerTask_BackSource_Partial_WithContentLength(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)

	testBytes, err := ioutil.ReadFile(test.File)
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
		})
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

	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	source.Register("http", sourceClient)
	defer source.UnRegister("http")
	sourceClient.EXPECT().GetContentLength(gomock.Any(), url, source.RequestHeader{}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, headers source.RequestHeader, rang *rangers.Range) (int64, error) {
			return int64(len(testBytes)), nil
		})
	sourceClient.EXPECT().Download(gomock.Any(), url, source.RequestHeader{}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, headers source.RequestHeader, rang *rangers.Range) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewBuffer(testBytes)), nil
		})

	pm := &pieceManager{
		calculateDigest: true,
		storageManager:  storageManager,
		pieceDownloader: downloader,
		computePieceSize: func(contentLength int64) uint32 {
			return uint32(pieceSize)
		},
	}
	ptm := &peerTaskManager{
		calculateDigest: true,
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager:     pm,
		storageManager:   storageManager,
		schedulerClient:  schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	req := &scheduler.PeerTaskRequest{
		Url: url,
		UrlMeta: &base.UrlMeta{
			Tag: "d7y-test",
		},
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	}
	ctx := context.Background()
	_, pt, _, err := newStreamPeerTask(ctx, ptm.host, pm, req,
		ptm.schedulerClient, ptm.schedulerOption, 0)
	assert.Nil(err, "new stream peer task")
	pt.SetCallback(&streamPeerTaskCallback{
		ptm:   ptm,
		pt:    pt,
		req:   req,
		start: time.Now(),
	})

	rc, _, err := pt.Start(ctx)
	assert.Nil(err, "start stream peer task")

	outputBytes, err := ioutil.ReadAll(rc)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
