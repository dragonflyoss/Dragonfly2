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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	testifyrequire "github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	mock_daemon "d7y.io/dragonfly/v2/client/daemon/test/mock/daemon"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	mock_scheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

func TestMain(m *testing.M) {
	logger.SetLevel(zapcore.DebugLevel)
	os.Exit(m.Run())
}

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
	getPieceTasks      bool
}

//go:generate mockgen -package mock_server_grpc -source ../../../pkg/rpc/dfdaemon/dfdaemon.pb.go -destination ../test/mock/daemongrpc/daemon_server_grpc.go
func setupPeerTaskManagerComponents(ctrl *gomock.Controller, opt componentsOption) (
	schedulerclient.SchedulerClient, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = mock_daemon.NewMockDaemonServer(ctrl)

	// 1.1 calculate piece digest and total digest
	r := bytes.NewBuffer(opt.content)
	var pieces = make([]string, int(math.Ceil(float64(len(opt.content))/float64(opt.pieceSize))))
	for i := range pieces {
		pieces[i] = digestutils.Md5Reader(io.LimitReader(r, int64(opt.pieceSize)))
	}
	totalDigests := digestutils.Sha256(pieces...)
	genPiecePacket := func(request *base.PieceTaskRequest) *base.PiecePacket {
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
		}
	}
	if opt.getPieceTasks {
		daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
				return genPiecePacket(request), nil
			})
		daemon.EXPECT().SyncPieceTasks(gomock.Any()).AnyTimes().DoAndReturn(func(arg0 dfdaemon.Daemon_SyncPieceTasksServer) error {
			return status.Error(codes.Unimplemented, "TODO")
		})
	} else {
		daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
				return nil, status.Error(codes.Unimplemented, "TODO")
			})
		daemon.EXPECT().SyncPieceTasks(gomock.Any()).AnyTimes().DoAndReturn(func(s dfdaemon.Daemon_SyncPieceTasksServer) error {
			request, err := s.Recv()
			if err != nil {
				return err
			}
			if err = s.Send(genPiecePacket(request)); err != nil {
				return err
			}
			for {
				request, err = s.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if err = s.Send(genPiecePacket(request)); err != nil {
					return err
				}
			}
			return nil
		})
	}
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
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (
			schedulerclient.PeerPacketStream, error) {
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
	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, opt)
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
			pieceDownloader: opt.pieceDownloader,
			computePieceSize: func(contentLength int64) uint32 {
				return opt.pieceSize
			},
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
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
	httpRange          *clientutil.Range // only used in back source cases
	pieceParallelCount int32
	pieceSize          int
	sizeScope          base.SizeScope
	peerID             string
	url                string
	legacyFeature      bool
	// when urlGenerator is not nil, use urlGenerator instead url
	// it's useful for httptest server
	urlGenerator func(ts *testSpec) string

	// mock schedule timeout
	peerPacketDelay []time.Duration
	scheduleTimeout time.Duration
	backSource      bool

	mockPieceDownloader  func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader
	mockHTTPSourceClient func(t *testing.T, ctrl *gomock.Controller, rg *clientutil.Range, taskData []byte, url string) source.ResourceClient

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

	taskTypes := []int{taskTypeConductor, taskTypeFile, taskTypeStream}
	taskTypeNames := []string{"conductor", "file", "stream"}

	testCases := []testSpec{
		{
			name:                 "normal size scope - p2p",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "normal-size-peer",
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
			peerID:               "small-size-peer",
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
			peerID:               "tiny-size-peer",
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
			peerID:              "normal-size-peer-back-source",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *clientutil.Range, taskData []byte, url string) source.ResourceClient {
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
			name:     "normal size scope - range - back source - content length",
			taskData: testBytes[0:4096],
			httpRange: &clientutil.Range{
				Start:  0,
				Length: 4096,
			},
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "normal-size-peer-range-back-source",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *clientutil.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourceMock.NewMockResourceClient(ctrl)
				sourceClient.EXPECT().GetContentLength(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (int64, error) {
						assert := testifyassert.New(t)
						if rg != nil {
							rgs, err := clientutil.ParseRange(request.Header.Get(headers.Range), math.MaxInt)
							assert.Nil(err)
							assert.Equal(1, len(rgs))
							assert.Equal(rg.String(), rgs[0].String())
						}
						return int64(len(taskData)), nil
					})
				sourceClient.EXPECT().Download(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (*source.Response, error) {
						assert := testifyassert.New(t)
						if rg != nil {
							rgs, err := clientutil.ParseRange(request.Header.Get(headers.Range), math.MaxInt)
							assert.Nil(err)
							assert.Equal(1, len(rgs))
							assert.Equal(rg.String(), rgs[0].String())
						}
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
			peerID:              "normal-size-peer-back-source-no-length",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *clientutil.Range, taskData []byte, url string) source.ResourceClient {
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
			name:                "normal size scope - back source - no content length - aligning",
			taskData:            testBytes[:8192],
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "normal-size-peer-back-source-aligning-no-length",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           base.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *clientutil.Range, taskData []byte, url string) source.ResourceClient {
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
			peerID:             "normal-size-peer-schedule-timeout",
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
			for _, legacy := range []bool{true, false} {
				for _, typ := range taskTypes {
					// dup a new test case with the task type
					logger.Infof("-------------------- test %s - type %s, legacy feature: %v started --------------------",
						_tc.name, taskTypeNames[typ], legacy)
					tc := _tc
					tc.taskType = typ
					tc.legacyFeature = legacy
					func() {
						ctrl := gomock.NewController(t)
						defer ctrl.Finish()
						mockContentLength := len(tc.taskData)

						urlMeta := &base.UrlMeta{
							Tag: "d7y-test",
						}

						if tc.httpRange != nil {
							urlMeta.Range = strings.TrimLeft(tc.httpRange.String(), "bytes=")
						}

						if tc.urlGenerator != nil {
							tc.url = tc.urlGenerator(&tc)
						}
						taskID := idgen.TaskID(tc.url, urlMeta)

						var (
							downloader   PieceDownloader
							sourceClient source.ResourceClient
						)

						if tc.mockPieceDownloader != nil {
							downloader = tc.mockPieceDownloader(ctrl, tc.taskData, tc.pieceSize)
						}

						if tc.mockHTTPSourceClient != nil {
							source.UnRegister("http")
							defer func() {
								// reset source client
								source.UnRegister("http")
								require.Nil(source.Register("http", httpprotocol.NewHTTPSourceClient(), httpprotocol.Adapter))
							}()
							// replace source client
							sourceClient = tc.mockHTTPSourceClient(t, ctrl, tc.httpRange, tc.taskData, tc.url)
							require.Nil(source.Register("http", sourceClient, httpprotocol.Adapter))
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
							getPieceTasks:      tc.legacyFeature,
						}
						// keep peer task running in enough time to check "getOrCreatePeerTaskConductor" always return same
						if tc.taskType == taskTypeConductor {
							option.peerPacketDelay = []time.Duration{time.Second}
						}
						mm := setupMockManager(ctrl, &tc, option)
						defer mm.CleanUp()

						tc.run(assert, require, mm, urlMeta)
					}()
					logger.Infof("-------------------- test %s - type %s, finished --------------------", _tc.name, taskTypeNames[typ])
				}
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

	peerTaskRequest := &scheduler.PeerTaskRequest{
		Url:      ts.url,
		UrlMeta:  urlMeta,
		PeerId:   ts.peerID,
		PeerHost: &scheduler.PeerHost{},
	}

	ptc, created, err := ptm.getOrCreatePeerTaskConductor(
		context.Background(), taskID, peerTaskRequest, rate.Limit(pieceSize*4), nil, nil, "")
	assert.Nil(err, "load first peerTaskConductor")
	assert.True(created, "should create a new peerTaskConductor")

	var ptcCount = 100
	var wg = &sync.WaitGroup{}
	wg.Add(ptcCount + 1)

	var result = make([]bool, ptcCount)

	go func(ptc *peerTaskConductor) {
		defer wg.Done()
		select {
		case <-time.After(5 * time.Minute):
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
		request := &scheduler.PeerTaskRequest{
			Url:      ts.url,
			UrlMeta:  urlMeta,
			PeerId:   fmt.Sprintf("should-not-use-peer-%d", i),
			PeerHost: &scheduler.PeerHost{},
		}
		p, created, err := ptm.getOrCreatePeerTaskConductor(
			context.Background(), taskID, request, rate.Limit(pieceSize*3), nil, nil, "")
		assert.Nil(err, fmt.Sprintf("load peerTaskConductor %d", i))
		assert.Equal(ptc.peerID, p.GetPeerID(), fmt.Sprintf("ptc %d should be same with ptc", i))
		assert.False(created, "should not create a new peerTaskConductor")
		go syncFunc(i, p)
	}

	require.Nil(ptc.start(), "peerTaskConductor start should be ok")

	switch ts.sizeScope {
	case base.SizeScope_TINY:
		require.NotNil(ptc.tinyData)
	case base.SizeScope_SMALL:
		require.NotNil(ptc.singlePiece)
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
	case <-time.After(5 * time.Minute):
		buf := make([]byte, 16384)
		buf = buf[:runtime.Stack(buf, true)]
		fmt.Printf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
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
		noRunningTask = true
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(noRunningTask, "no running tasks")

	// test reuse stream task
	rc, _, ok := ptm.tryReuseStreamPeerTask(context.Background(),
		&StreamTaskRequest{
			URL:     ts.url,
			URLMeta: urlMeta,
			PeerID:  ts.peerID,
		})
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
