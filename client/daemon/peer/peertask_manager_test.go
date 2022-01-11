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
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	mock_daemon "d7y.io/dragonfly/v2/client/daemon/test/mock/daemon"
	mock_scheduler "d7y.io/dragonfly/v2/client/daemon/test/mock/scheduler"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
)

type componentsOption struct {
	taskID             string
	contentLength      int64
	pieceSize          uint32
	pieceParallelCount int32
	peerPacketDelay    []time.Duration
	backSource         bool
	scope              base.SizeScope
	tinyContent        []byte
}

func setupPeerTaskManagerComponents(ctrl *gomock.Controller, opt componentsOption) (
	schedulerclient.SchedulerClient, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = mock_daemon.NewMockDaemonServer(ctrl)
	daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var tasks []*base.PieceInfo
		for i := uint32(0); i < request.Limit; i++ {
			start := opt.pieceSize * (request.StartNum + i)
			if int64(start)+1 > opt.contentLength {
				break
			}
			size := opt.pieceSize
			if int64(start+opt.pieceSize) > opt.contentLength {
				size = uint32(opt.contentLength) - start
			}
			tasks = append(tasks,
				&base.PieceInfo{
					PieceNum:    int32(request.StartNum + i),
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
			ContentLength: opt.contentLength,
			TotalPiece:    int32(math.Ceil(float64(opt.contentLength) / float64(opt.pieceSize))),
		}, nil
	})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})

	go func() {
		if err := daemonserver.New(daemon).Serve(ln); err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := mock_scheduler.NewMockPeerPacketStream(ctrl)
	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *scheduler.PieceResult) error {
			return nil
		})
	var (
		delayCount int
		sent       = make(chan struct{}, 1)
	)
	sent <- struct{}{}
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*scheduler.PeerPacket, error) {
			if len(opt.peerPacketDelay) > delayCount {
				if delay := opt.peerPacketDelay[delayCount]; delay > 0 {
					time.Sleep(delay)
				}
				delayCount++
			}
			<-sent
			if opt.backSource {
				return nil, dferrors.Newf(base.Code_SchedNeedBackSource, "fake back source error")
			}
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
			if opt.scope == base.SizeScope_TINY {
				return &scheduler.RegisterResult{
					TaskId:    opt.taskID,
					SizeScope: base.SizeScope_TINY,
					DirectPiece: &scheduler.RegisterResult_PieceContent{
						PieceContent: opt.tinyContent,
					},
				}, nil
			}
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
	tempDir, _ := os.MkdirTemp("", "d7y-test-*")
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

func TestPeerTaskManager_getOrCreatePeerTaskConductor(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
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

	request := &scheduler.PeerTaskRequest{
		Url: "http://localhost/test/data",
		UrlMeta: &base.UrlMeta{
			Tag: "d7y-test",
		},
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	}
	taskID = idgen.TaskID(request.Url, request.UrlMeta)

	schedulerClient, storageManager := setupPeerTaskManagerComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
		})
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).Times(
		int(math.Ceil(float64(len(testBytes)) / float64(pieceSize)))).DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := io.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
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

	ptc, err := ptm.getOrCreatePeerTaskConductor(context.Background(), taskID, request, rate.Limit(pieceSize*4))
	assert.Nil(err, "load first peerTaskConductor")

	var ptcCount = 10
	var wg = &sync.WaitGroup{}
	wg.Add(ptcCount + 1)

	var result = make([]bool, ptcCount)

	go func(ptc *peerTaskConductor) {
		defer wg.Done()
		select {
		case <-time.After(10 * time.Second):
			ptc.Fail()
		case <-ptc.successCh:
			return
		case <-ptc.failCh:
			return
		}
	}(ptc)

	syncFunc := func(i int, ptc *peerTaskConductor) {
		pieceCh := ptc.broker.Subscribe()
		defer wg.Done()
		for {
			select {
			case <-pieceCh:
			case <-ptc.successCh:
				result[i] = true
				return
			case <-ptc.failCh:
				return
			}
		}
	}

	for i := 0; i < ptcCount; i++ {
		p, err := ptm.getOrCreatePeerTaskConductor(context.Background(), taskID, request, rate.Limit(pieceSize*3))
		assert.Nil(err, fmt.Sprintf("load peerTaskConductor %d", i))
		assert.Equal(ptc, p, fmt.Sprintf("ptc %d should be same with ptc", i))
		go syncFunc(i, p)
	}

	wg.Wait()

	for i, r := range result {
		assert.True(r, fmt.Sprintf("task %d result should be true", i))
	}

	var taskCount int
	ptm.runningPeerTasks.Range(func(key, value interface{}) bool {
		taskCount++
		return true
	})
	assert.Equal(0, taskCount, "no running tasks")

	// test reuse stream task
	rc, _, ok := ptm.tryReuseStreamPeerTask(context.Background(), request)
	assert.True(ok, "reuse stream task")
	defer rc.Close()

	data, err := io.ReadAll(rc)
	assert.Nil(err, "read all should be ok")
	assert.Equal(testBytes, data, "stream output and desired output must match")

	// test file task
	progress, ok := ptm.tryReuseFilePeerTask(
		context.Background(),
		&FilePeerTaskRequest{
			PeerTaskRequest: scheduler.PeerTaskRequest{
				Url: "http://localhost/test/data",
				UrlMeta: &base.UrlMeta{
					Tag: "d7y-test",
				},
				PeerId:   peerID,
				PeerHost: &scheduler.PeerHost{},
			},
			Output: output,
		})

	assert.True(ok, "reuse file task")
	var p *FilePeerTaskProgress
	select {
	case p = <-progress:
	default:
	}

	assert.NotNil(p, "progress should not be nil")
	outputBytes, err := os.ReadFile(output)
	assert.Nil(err, "load output file should be ok")
	assert.Equal(testBytes, outputBytes, "file output and desired output must match")
}

func TestPeerTaskManager_StartFilePeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
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

	schedulerClient, storageManager := setupPeerTaskManagerComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
		})
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).Times(
		int(math.Ceil(float64(len(testBytes)) / float64(pieceSize)))).DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := io.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
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
	progress, _, err := ptm.StartFilePeerTask(
		context.Background(),
		&FilePeerTaskRequest{
			PeerTaskRequest: scheduler.PeerTaskRequest{
				Url: "http://localhost/test/data",
				UrlMeta: &base.UrlMeta{
					Tag: "d7y-test",
				},
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

	outputBytes, err := os.ReadFile(output)
	assert.Nil(err, "load output file")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartFilePeerTask_SizeScope_Tiny(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")
	testBytes = testBytes[:64]

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

	schedulerClient, storageManager := setupPeerTaskManagerComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
			scope:              base.SizeScope_TINY,
			tinyContent:        testBytes,
		})
	defer storageManager.CleanUp()

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: nil,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	progress, _, err := ptm.StartFilePeerTask(
		context.Background(),
		&FilePeerTaskRequest{
			PeerTaskRequest: scheduler.PeerTaskRequest{
				Url: "http://localhost/test/data",
				UrlMeta: &base.UrlMeta{
					Tag: "d7y-test",
				},
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

	outputBytes, err := os.ReadFile(output)
	assert.Nil(err, "load output file")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartStreamPeerTask(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"
	)
	sched, storageManager := setupPeerTaskManagerComponents(
		ctrl,
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
			rc := io.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
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

	r, _, err := ptm.StartStreamPeerTask(
		context.Background(),
		&scheduler.PeerTaskRequest{
			Url: "http://localhost/test/data",
			UrlMeta: &base.UrlMeta{
				Tag: "d7y-test",
			},
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		})
	assert.Nil(err, "start stream peer task")

	outputBytes, err := io.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartStreamPeerTask_SizeScope_Tiny(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")
	testBytes = testBytes[:64]

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"
	)
	sched, storageManager := setupPeerTaskManagerComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
			scope:              base.SizeScope_TINY,
			tinyContent:        testBytes,
		})
	defer storageManager.CleanUp()

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: nil,
		},
		storageManager:  storageManager,
		schedulerClient: sched,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}

	r, _, err := ptm.StartStreamPeerTask(
		context.Background(),
		&scheduler.PeerTaskRequest{
			Url: "http://localhost/test/data",
			UrlMeta: &base.UrlMeta{
				Tag: "d7y-test",
			},
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		})
	assert.Nil(err, "start stream peer task")

	outputBytes, err := io.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestPeerTaskManager_StartStreamPeerTask_BackSource(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-0"
		taskID = "task-0"
	)

	source.UnRegister("http")
	assert.Nil(source.Register("http", httpprotocol.NewHTTPSourceClient(), httpprotocol.Adapter))
	defer source.UnRegister("http")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := w.Write(testBytes)
		assert.Nil(err)
		assert.Equal(mockContentLength, n)
	}))
	defer ts.Close()

	sched, storageManager := setupPeerTaskManagerComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
			peerPacketDelay:    []time.Duration{time.Second},
		})
	defer storageManager.CleanUp()

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:   storageManager,
			pieceDownloader:  NewMockPieceDownloader(ctrl),
			computePieceSize: util.ComputePieceSize,
		},
		storageManager:  storageManager,
		schedulerClient: sched,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: time.Nanosecond},
		},
	}

	r, _, err := ptm.StartStreamPeerTask(
		context.Background(),
		&scheduler.PeerTaskRequest{
			Url: ts.URL,
			UrlMeta: &base.UrlMeta{
				Tag: "d7y-test",
			},
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		})
	assert.Nil(err, "start stream peer task")

	outputBytes, err := io.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
