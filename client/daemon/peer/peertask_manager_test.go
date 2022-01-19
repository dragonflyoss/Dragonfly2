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
	testifyrequire "github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	daemonserverMock "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server/mocks"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	schedulerclientMock "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	schedulerMock "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

type componentsOption struct {
	taskID             string
	contentLength      int64
	pieceSize          uint32
	pieceParallelCount int32
	pieceDownloader    PieceDownloader
	sourceClient       source.ResourceClient
	peerPacketDelay    []time.Duration
	backSource         bool
	scope              base.SizeScope
	content            []byte
}

func setupPeerTaskManagerComponents(ctrl *gomock.Controller, opt componentsOption) (
	schedulerclient.SchedulerClient, dfclient.ElasticClient, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemonServer = daemonserverMock.NewMockDaemonServer(ctrl)

	// 1.1 calculate piece digest and total digest
	r := bytes.NewBuffer(opt.content)
	var pieces = make([]string, int(math.Ceil(float64(len(opt.content))/float64(opt.pieceSize))))
	for i := range pieces {
		pieces[i] = digestutils.Md5Reader(io.LimitReader(r, int64(opt.pieceSize)))
	}
	totalDigests := digestutils.Sha256(pieces...)
	daemonServer.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
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
						PieceMd5:    pieces[request.StartNum+i],
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
				PieceMd5Sign:  totalDigests,
			}, nil
		})
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})

	go func() {
		if err := daemonserver.New(daemonServer).Serve(ln); err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := schedulerMock.NewMockScheduler_ReportPieceResultClient(ctrl)
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
	schedulerClient := schedulerclientMock.NewMockSchedulerClient(ctrl)
	schedulerClient.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
			switch opt.scope {
			case base.SizeScope_TINY:
				return &scheduler.RegisterResult{
					TaskId:    opt.taskID,
					SizeScope: base.SizeScope_TINY,
					DirectPiece: &scheduler.RegisterResult_PieceContent{
						PieceContent: opt.content,
					},
				}, nil
			case base.SizeScope_SMALL:
				return &scheduler.RegisterResult{
					TaskId:    opt.taskID,
					SizeScope: base.SizeScope_SMALL,
					DirectPiece: &scheduler.RegisterResult_SinglePiece{
						SinglePiece: &scheduler.SinglePiece{
							DstPid:  "fake-pid",
							DstAddr: "fake-addr",
							PieceInfo: &base.PieceInfo{
								PieceNum:    0,
								RangeStart:  0,
								RangeSize:   uint32(opt.contentLength),
								PieceMd5:    pieces[0],
								PieceOffset: 0,
								PieceStyle:  0,
							},
						},
					},
				}, nil
			}
			return &scheduler.RegisterResult{
				TaskId:      opt.taskID,
				SizeScope:   base.SizeScope_NORMAL,
				DirectPiece: nil,
			}, nil
		})
	schedulerClient.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error) {
			return pps, nil
		})
	schedulerClient.EXPECT().ReportPeerResult(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
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
	peerTaskClient, _ := dfclient.GetElasticClient()
	return schedulerClient, peerTaskClient, storageManager
}

type mockManager struct {
	testSpec        *testSpec
	peerTaskManager *peerTaskManager
	schedulerClient schedulerclient.SchedulerClient
	storageManager  storage.Manager
}

func (m *mockManager) CleanUp() {
	m.storageManager.CleanUp()
	for _, f := range m.testSpec.cleanUp {
		f()
	}
}

func setupMockManager(ctrl *gomock.Controller, ts *testSpec, opt componentsOption) *mockManager {
	schedulerClient, peerTaskClient, storageManager := setupPeerTaskManagerComponents(ctrl, opt)
	scheduleTimeout := clientutil.Duration{Duration: 10 * time.Minute}
	if ts.scheduleTimeout > 0 {
		scheduleTimeout = clientutil.Duration{Duration: ts.scheduleTimeout}
	}
	ptm := &peerTaskManager{
		calculateDigest: true,
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			calculateDigest: true,
			storageManager:  storageManager,
			pieceDownloader: opt.pieceDownloader,
			computePieceSize: func(contentLength int64) uint32 {
				return opt.pieceSize
			},
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		peerTaskClient:  peerTaskClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: scheduleTimeout,
		},
	}
	return &mockManager{
		testSpec:        ts,
		peerTaskManager: ptm,
		schedulerClient: schedulerClient,
		storageManager:  storageManager,
	}
}

const (
	taskTypeFile = iota
	taskTypeStream
	taskTypeConductor
)

type testSpec struct {
	taskType           int
	name               string
	taskData           []byte
	pieceParallelCount int32
	pieceSize          int
	sizeScope          base.SizeScope
	peerID             string
	url                string
	// when urlGenerator is not nil, use urlGenerator instead url
	// it's useful for httptest server
	urlGenerator func(ts *testSpec) string

	// mock schedule timeout
	peerPacketDelay []time.Duration
	scheduleTimeout time.Duration
	backSource      bool

	mockPieceDownloader  func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader
	mockHTTPSourceClient func(ctrl *gomock.Controller, taskData []byte, url string) source.ResourceClient

	cleanUp []func()
}

func TestPeerTaskManager_TaskSuite(t *testing.T) {
	assert := testifyassert.New(t)
	require := testifyrequire.New(t)
	testBytes, err := os.ReadFile(test.File)
	require.Nil(err, "load test file")

	commonPieceDownloader := func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader {
		downloader := NewMockPieceDownloader(ctrl)
		downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).Times(
			int(math.Ceil(float64(len(taskData)) / float64(pieceSize)))).DoAndReturn(
			func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
				rc := io.NopCloser(
					bytes.NewBuffer(
						taskData[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
					))
				return rc, rc, nil
			})
		return downloader
	}

	taskTypes := []int{taskTypeFile, taskTypeStream, taskTypeConductor}

	testCases := []testSpec{
		{
			name:                 "normal size scope - p2p",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "peer-0",
			url:                  "http://localhost/test/data",
			sizeScope:            base.SizeScope_NORMAL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name:                 "small size scope - p2p",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            16384,
			peerID:               "peer-0",
			url:                  "http://localhost/test/data",
			sizeScope:            base.SizeScope_SMALL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name:                 "tidy size scope - p2p",
			taskData:             testBytes[:64],
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "peer-0",
			url:                  "http://localhost/test/data",
			sizeScope:            base.SizeScope_TINY,
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: nil,
		},
		{
			name:                "normal size scope - back source - content length",
			taskData:            testBytes,
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "peer-0",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(ctrl *gomock.Controller, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourceMock.NewMockResourceClient(ctrl)
				sourceClient.EXPECT().GetContentLength(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (int64, error) {
						return int64(len(taskData)), nil
					})
				sourceClient.EXPECT().Download(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (*source.Response, error) {
						return source.NewResponse(io.NopCloser(bytes.NewBuffer(taskData))), nil
					})
				return sourceClient
			},
		},
		{
			name:                "normal size scope - back source - no content length",
			taskData:            testBytes,
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "peer-0",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(ctrl *gomock.Controller, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourceMock.NewMockResourceClient(ctrl)
				sourceClient.EXPECT().GetContentLength(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (int64, error) {
						return -1, nil
					})
				sourceClient.EXPECT().Download(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (*source.Response, error) {
						return source.NewResponse(io.NopCloser(bytes.NewBuffer(taskData))), nil
					})
				return sourceClient
			},
		},
		{
			name:                "normal size scope - back source - content length - aligning",
			taskData:            testBytes[:8192],
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "peer-0",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(ctrl *gomock.Controller, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourceMock.NewMockResourceClient(ctrl)
				sourceClient.EXPECT().GetContentLength(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (int64, error) {
						return -1, nil
					})
				sourceClient.EXPECT().Download(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (*source.Response, error) {
						return source.NewResponse(io.NopCloser(bytes.NewBuffer(taskData))), nil
					})
				return sourceClient
			},
		},
		{
			name:               "normal size scope - schedule timeout - auto back source",
			taskData:           testBytes,
			pieceParallelCount: 4,
			pieceSize:          1024,
			peerID:             "peer-0",
			peerPacketDelay:    []time.Duration{time.Second},
			scheduleTimeout:    time.Nanosecond,
			urlGenerator: func(ts *testSpec) string {
				server := httptest.NewServer(http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						n, err := w.Write(testBytes)
						assert.Nil(err)
						assert.Equal(len(ts.taskData), n)
					}))
				ts.cleanUp = append(ts.cleanUp, func() {
					server.Close()
				})
				return server.URL
			},
			sizeScope:            base.SizeScope_NORMAL,
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: nil,
		},
	}

	for _, _tc := range testCases {
		t.Run(_tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			require := testifyrequire.New(t)
			for _, typ := range taskTypes {
				// dup a new test case with the task type
				tc := _tc
				tc.taskType = typ
				func() {
					ctrl := gomock.NewController(t)
					defer ctrl.Finish()
					mockContentLength := len(tc.taskData)

					urlMeta := &base.UrlMeta{
						Tag: "d7y-test",
					}
					if tc.urlGenerator != nil {
						tc.url = tc.urlGenerator(&tc)
					}
					taskID := idgen.TaskID(tc.url, urlMeta)

					if tc.mockHTTPSourceClient != nil {
						source.UnRegister("http")
						defer func() {
							// reset source client
							source.UnRegister("http")
							require.Nil(source.Register("http", httpprotocol.NewHTTPSourceClient(), httpprotocol.Adapter))
						}()
						// replace source client
						require.Nil(source.Register("http", tc.mockHTTPSourceClient(ctrl, tc.taskData, tc.url), httpprotocol.Adapter))
					}

					var (
						downloader   PieceDownloader
						sourceClient source.ResourceClient
					)
					if tc.mockPieceDownloader != nil {
						downloader = tc.mockPieceDownloader(ctrl, tc.taskData, tc.pieceSize)
					}
					if tc.mockHTTPSourceClient != nil {
						sourceClient = tc.mockHTTPSourceClient(ctrl, tc.taskData, tc.url)
					}

					option := componentsOption{
						taskID:             taskID,
						contentLength:      int64(mockContentLength),
						pieceSize:          uint32(tc.pieceSize),
						pieceParallelCount: tc.pieceParallelCount,
						pieceDownloader:    downloader,
						sourceClient:       sourceClient,
						content:            tc.taskData,
						scope:              tc.sizeScope,
						peerPacketDelay:    tc.peerPacketDelay,
						backSource:         tc.backSource,
					}
					// keep peer task running in enough time to check "getOrCreatePeerTaskConductor" always return same
					if tc.taskType == taskTypeConductor {
						option.peerPacketDelay = []time.Duration{time.Second}
					}
					mm := setupMockManager(ctrl, &tc, option)
					defer mm.CleanUp()

					tc.run(assert, require, mm, urlMeta)
				}()
			}
		})
	}
}

func (ts *testSpec) run(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *base.UrlMeta) {
	switch ts.taskType {
	case taskTypeFile:
		ts.runFileTaskTest(assert, require, mm, urlMeta)
	case taskTypeStream:
		ts.runStreamTaskTest(assert, require, mm, urlMeta)
	case taskTypeConductor:
		ts.runConductorTest(assert, require, mm, urlMeta)
	default:
		panic("unknown test type")
	}
}

func (ts *testSpec) runFileTaskTest(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *base.UrlMeta) {
	var output = "../test/testdata/test.output"
	defer func() {
		assert.Nil(os.Remove(output))
	}()
	progress, _, err := mm.peerTaskManager.StartFileTask(
		context.Background(),
		&FileTaskRequest{
			PeerTaskRequest: scheduler.PeerTaskRequest{
				Url:      ts.url,
				UrlMeta:  urlMeta,
				PeerId:   ts.peerID,
				PeerHost: &scheduler.PeerHost{},
			},
			Output: output,
		})
	require.Nil(err, "start file peer task")

	var p *FileTaskProgress
	for p = range progress {
		require.True(p.State.Success)
		if p.PeerTaskDone {
			p.DoneCallback()
			break
		}
	}
	require.NotNil(p)
	require.True(p.PeerTaskDone)

	outputBytes, err := os.ReadFile(output)
	require.Nil(err, "load output file")
	require.Equal(ts.taskData, outputBytes, "output and desired output must match")
}

func (ts *testSpec) runStreamTaskTest(_ *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *base.UrlMeta) {
	r, _, err := mm.peerTaskManager.StartStreamTask(
		context.Background(),
		&StreamTaskRequest{
			URL:     ts.url,
			URLMeta: urlMeta,
			PeerID:  ts.peerID,
		})
	require.Nil(err, "start stream peer task")

	outputBytes, err := io.ReadAll(r)
	require.Nil(err, "load read data")
	require.Equal(ts.taskData, outputBytes, "output and desired output must match")
}

func (ts *testSpec) runConductorTest(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *base.UrlMeta) {
	var (
		ptm       = mm.peerTaskManager
		pieceSize = ts.pieceSize
		taskID    = idgen.TaskID(ts.url, urlMeta)
		output    = "../test/testdata/test.output"
	)
	defer func() {
		assert.Nil(os.Remove(output))
	}()

	request := &scheduler.PeerTaskRequest{
		Url:      ts.url,
		UrlMeta:  urlMeta,
		PeerId:   ts.peerID,
		PeerHost: &scheduler.PeerHost{},
	}

	ptc, err := ptm.getOrCreatePeerTaskConductor(context.Background(), taskID, request, rate.Limit(pieceSize*4))
	assert.Nil(err, "load first peerTaskConductor")

	switch ts.sizeScope {
	case base.SizeScope_TINY:
		require.NotNil(ptc.tinyData)
	case base.SizeScope_SMALL:
		require.NotNil(ptc.singlePiece)
	}

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
		assert.Equal(ptc.peerID, p.GetPeerID(), fmt.Sprintf("ptc %d should be same with ptc", i))
		go syncFunc(i, p)
	}

	wg.Wait()

	for i, r := range result {
		assert.True(r, fmt.Sprintf("task %d result should be true", i))
	}

	var (
		noRunningTask = true
		success       bool
	)
	select {
	case <-ptc.successCh:
		success = true
	case <-ptc.failCh:
	case <-time.After(10 * time.Minute):
	}
	assert.True(success, "task should success")

	for i := 0; i < 3; i++ {
		ptm.runningPeerTasks.Range(func(key, value interface{}) bool {
			noRunningTask = false
			return false
		})
		if noRunningTask {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(noRunningTask, "no running tasks")

	// test reuse stream task
	rc, _, ok := ptm.tryReuseStreamPeerTask(context.Background(), request)
	assert.True(ok, "reuse stream task")
	defer func() {
		assert.Nil(rc.Close())
	}()

	data, err := io.ReadAll(rc)
	assert.Nil(err, "read all should be ok")
	assert.Equal(ts.taskData, data, "stream output and desired output must match")

	// test reuse file task
	progress, ok := ptm.tryReuseFilePeerTask(
		context.Background(),
		&FileTaskRequest{
			PeerTaskRequest: scheduler.PeerTaskRequest{
				Url:      ts.url,
				UrlMeta:  urlMeta,
				PeerId:   ts.peerID,
				PeerHost: &scheduler.PeerHost{},
			},
			Output: output,
		})

	assert.True(ok, "reuse file task")
	var p *FileTaskProgress
	select {
	case p = <-progress:
	default:
	}

	assert.NotNil(p, "progress should not be nil")
	outputBytes, err := os.ReadFile(output)
	assert.Nil(err, "load output file should be ok")
	assert.Equal(ts.taskData, outputBytes, "file output and desired output must match")
}
