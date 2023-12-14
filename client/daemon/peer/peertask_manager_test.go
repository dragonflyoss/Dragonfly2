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
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	testifyrequire "github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/status"

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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc"
	daemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	schedulerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/clients/httpprotocol"
	sourcemocks "d7y.io/dragonfly/v2/pkg/source/mocks"
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
	scope              commonv1.SizeScope
	content            []byte
	reregister         bool
}

func setupPeerTaskManagerComponents(ctrl *gomock.Controller, opt componentsOption) (
	schedulerclient.V1, storage.Manager) {
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = dfdaemonv1mocks.NewMockDaemonServer(ctrl)

	// 1.1 calculate piece digest and total digest
	r := bytes.NewBuffer(opt.content)
	var pieces = make([]string, int(math.Ceil(float64(len(opt.content))/float64(opt.pieceSize))))
	for i := range pieces {
		pieces[i] = digest.MD5FromReader(io.LimitReader(r, int64(opt.pieceSize)))
	}
	totalDigests := digest.SHA256FromStrings(pieces...)
	genPiecePacket := func(request *commonv1.PieceTaskRequest) *commonv1.PiecePacket {
		var tasks []*commonv1.PieceInfo
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
				&commonv1.PieceInfo{
					PieceNum:    int32(request.StartNum + i),
					RangeStart:  uint64(start),
					RangeSize:   size,
					PieceMd5:    pieces[request.StartNum+i],
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		return &commonv1.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    tasks,
			ContentLength: opt.contentLength,
			TotalPiece:    int32(math.Ceil(float64(opt.contentLength) / float64(opt.pieceSize))),
			PieceMd5Sign:  totalDigests,
		}
	}
	daemon.EXPECT().GetPieceTasks(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
			return nil, status.Error(codes.Unimplemented, "TODO")
		})
	daemon.EXPECT().SyncPieceTasks(gomock.Any()).AnyTimes().DoAndReturn(func(s dfdaemonv1.Daemon_SyncPieceTasksServer) error {
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
	ln, _ := rpc.Listen(dfnet.NetAddr{
		Type: "tcp",
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
	})

	go func() {
		hs := health.NewServer()
		if err := daemonserver.New(daemon, hs).Serve(ln); err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// 2. setup a scheduler
	pps := schedulerv1mocks.NewMockScheduler_ReportPieceResultClient(ctrl)
	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *schedulerv1.PieceResult) error {
			return nil
		})
	var (
		delayCount int
		sent       = make(chan struct{}, 1)
	)
	sent <- struct{}{}
	var reregistered bool
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*schedulerv1.PeerPacket, error) {
			if opt.reregister && !reregistered {
				reregistered = true
				return nil, dferrors.New(commonv1.Code_SchedReregister, "reregister")
			}
			if len(opt.peerPacketDelay) > delayCount {
				if delay := opt.peerPacketDelay[delayCount]; delay > 0 {
					time.Sleep(delay)
				}
				delayCount++
			}
			<-sent
			if opt.backSource {
				return nil, dferrors.Newf(commonv1.Code_SchedNeedBackSource, "fake back source error")
			}
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

	sched := schedulerclientmocks.NewMockV1(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
			switch opt.scope {
			case commonv1.SizeScope_TINY:
				return &schedulerv1.RegisterResult{
					TaskId:    opt.taskID,
					SizeScope: commonv1.SizeScope_TINY,
					DirectPiece: &schedulerv1.RegisterResult_PieceContent{
						PieceContent: opt.content,
					},
				}, nil
			case commonv1.SizeScope_SMALL:
				return &schedulerv1.RegisterResult{
					TaskId:    opt.taskID,
					SizeScope: commonv1.SizeScope_SMALL,
					DirectPiece: &schedulerv1.RegisterResult_SinglePiece{
						SinglePiece: &schedulerv1.SinglePiece{
							DstPid:  "fake-pid",
							DstAddr: "fake-addr",
							PieceInfo: &commonv1.PieceInfo{
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
			return &schedulerv1.RegisterResult{
				TaskId:      opt.taskID,
				SizeScope:   commonv1.SizeScope_NORMAL,
				DirectPiece: nil,
			}, nil
		})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (
			schedulerv1.Scheduler_ReportPieceResultClient, error) {
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

type mockManager struct {
	testSpec        *testSpec
	peerTaskManager *peerTaskManager
	schedulerClient schedulerclient.V1
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
	scheduleTimeout := util.Duration{Duration: 10 * time.Minute}
	if ts.scheduleTimeout > 0 {
		scheduleTimeout = util.Duration{Duration: ts.scheduleTimeout}
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
				PieceManager: &pieceManager{
					calculateDigest: true,
					pieceDownloader: opt.pieceDownloader,
					computePieceSize: func(contentLength int64) uint32 {
						return opt.pieceSize
					},
				},
				StorageManager: storageManager,
				SchedulerOption: config.SchedulerOption{
					ScheduleTimeout: scheduleTimeout,
				},
				GRPCDialTimeout: time.Second,
				GRPCCredentials: insecure.NewCredentials(),
			},
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
	taskTypeSeed
)

type testSpec struct {
	runTaskTypes       []int
	taskType           int
	name               string
	taskData           []byte
	httpRange          *nethttp.Range // only used in back source cases
	pieceParallelCount int32
	pieceSize          int
	sizeScope          commonv1.SizeScope
	peerID             string
	url                string
	reregister         bool
	// when urlGenerator is not nil, use urlGenerator instead url
	// it's useful for httptest server
	urlGenerator func(ts *testSpec) string

	// mock schedule timeout
	peerPacketDelay []time.Duration
	scheduleTimeout time.Duration
	backSource      bool

	mockPieceDownloader  func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader
	mockHTTPSourceClient func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient

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

	taskTypes := []int{taskTypeConductor, taskTypeFile, taskTypeStream} // seed task need back source client
	taskTypeNames := []string{"conductor", "file", "stream", "seed"}

	testCases := []testSpec{
		{
			name:                 "normal size scope - p2p",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "normal-size-peer",
			url:                  "http://localhost/test/data",
			sizeScope:            commonv1.SizeScope_NORMAL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name:                 "normal size scope - p2p - reregister",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "normal-size-peer",
			url:                  "http://localhost/test/data",
			sizeScope:            commonv1.SizeScope_NORMAL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
			reregister:           true,
		},
		{
			name:                 "small size scope - p2p",
			taskData:             testBytes,
			pieceParallelCount:   4,
			pieceSize:            16384,
			peerID:               "small-size-peer",
			url:                  "http://localhost/test/data",
			sizeScope:            commonv1.SizeScope_SMALL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name:                 "tiny size scope - p2p",
			taskData:             testBytes[:64],
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "tiny-size-peer",
			url:                  "http://localhost/test/data",
			sizeScope:            commonv1.SizeScope_TINY,
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: nil,
		},
		{
			name:                 "empty file - p2p",
			taskData:             []byte{},
			pieceParallelCount:   4,
			pieceSize:            1024,
			peerID:               "empty-file-peer",
			url:                  "http://localhost/test/data",
			sizeScope:            commonv1.SizeScope_NORMAL,
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name:                "normal size scope - back source - content length",
			runTaskTypes:        []int{taskTypeConductor, taskTypeFile, taskTypeStream, taskTypeSeed},
			taskData:            testBytes,
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "normal-size-peer-back-source",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           commonv1.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourcemocks.NewMockResourceClient(ctrl)
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
			name:               "normal size scope - range - back source - content length",
			runTaskTypes:       []int{taskTypeConductor, taskTypeFile, taskTypeStream, taskTypeSeed},
			taskData:           testBytes[0:4096],
			pieceParallelCount: 4,
			pieceSize:          1024,
			peerID:             "normal-size-peer-range-back-source",
			backSource:         true,
			url:                "http://localhost/test/data",
			sizeScope:          commonv1.SizeScope_NORMAL,
			httpRange: &nethttp.Range{
				Start:  0,
				Length: 4096,
			},
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourcemocks.NewMockResourceClient(ctrl)
				sourceClient.EXPECT().GetContentLength(source.RequestEq(url)).AnyTimes().DoAndReturn(
					func(request *source.Request) (int64, error) {
						assert := testifyassert.New(t)
						if rg != nil {
							rgs, err := nethttp.ParseRange(request.Header.Get(headers.Range), math.MaxInt64)
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
							rgs, err := nethttp.ParseRange(request.Header.Get(headers.Range), math.MaxInt64)
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
			runTaskTypes:        []int{taskTypeConductor, taskTypeFile, taskTypeStream, taskTypeSeed},
			taskData:            testBytes,
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "normal-size-peer-back-source-no-length",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           commonv1.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourcemocks.NewMockResourceClient(ctrl)
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
			runTaskTypes:        []int{taskTypeConductor, taskTypeFile, taskTypeStream, taskTypeSeed},
			taskData:            testBytes[:8192],
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "normal-size-peer-back-source-aligning-no-length",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           commonv1.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourcemocks.NewMockResourceClient(ctrl)
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
			sizeScope:            commonv1.SizeScope_NORMAL,
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: nil,
		},
		{
			name:                "empty file peer - back source - content length",
			runTaskTypes:        []int{taskTypeConductor, taskTypeFile, taskTypeStream, taskTypeSeed},
			taskData:            []byte{},
			pieceParallelCount:  4,
			pieceSize:           1024,
			peerID:              "empty-file-peer-back-source",
			backSource:          true,
			url:                 "http://localhost/test/data",
			sizeScope:           commonv1.SizeScope_NORMAL,
			mockPieceDownloader: nil,
			mockHTTPSourceClient: func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
				sourceClient := sourcemocks.NewMockResourceClient(ctrl)
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
	}

	for _, _tc := range testCases {
		t.Run(_tc.name, func(t *testing.T) {
			var types = _tc.runTaskTypes
			if _tc.runTaskTypes == nil {
				types = taskTypes
			}
			assert = testifyassert.New(t)
			require = testifyrequire.New(t)
			for _, typ := range types {
				// dup a new test case with the task type
				logger.Infof("-------------------- test %s - type %s, started --------------------",
					_tc.name, taskTypeNames[typ])
				tc := _tc
				tc.taskType = typ
				func() {
					ctrl := gomock.NewController(t)
					defer ctrl.Finish()
					mockContentLength := len(tc.taskData)

					urlMeta := &commonv1.UrlMeta{
						Tag: "d7y-test",
					}

					if tc.httpRange != nil {
						urlMeta.Range = strings.TrimPrefix(tc.httpRange.String(), "bytes=")
					}

					if tc.urlGenerator != nil {
						tc.url = tc.urlGenerator(&tc)
					}
					taskID := idgen.TaskIDV1(tc.url, urlMeta)

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
						reregister:         tc.reregister,
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
		})
	}
}

func (ts *testSpec) run(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *commonv1.UrlMeta) {
	switch ts.taskType {
	case taskTypeFile:
		ts.runFileTaskTest(assert, require, mm, urlMeta)
	case taskTypeStream:
		ts.runStreamTaskTest(assert, require, mm, urlMeta)
	case taskTypeConductor:
		ts.runConductorTest(assert, require, mm, urlMeta)
	case taskTypeSeed:
		ts.runSeedTaskTest(assert, require, mm, urlMeta)
	default:
		panic("unknown test type")
	}
}

func (ts *testSpec) runFileTaskTest(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *commonv1.UrlMeta) {
	var output = "../test/testdata/test.output"
	defer func() {
		assert.Nil(os.Remove(output))
	}()
	progress, err := mm.peerTaskManager.StartFileTask(
		context.Background(),
		&FileTaskRequest{
			PeerTaskRequest: schedulerv1.PeerTaskRequest{
				Url:      ts.url,
				UrlMeta:  urlMeta,
				PeerId:   ts.peerID,
				PeerHost: &schedulerv1.PeerHost{},
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

func (ts *testSpec) runStreamTaskTest(_ *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *commonv1.UrlMeta) {
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

func (ts *testSpec) runSeedTaskTest(_ *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *commonv1.UrlMeta) {
	r, _, err := mm.peerTaskManager.StartSeedTask(
		context.Background(),
		&SeedTaskRequest{
			PeerTaskRequest: schedulerv1.PeerTaskRequest{
				Url:         ts.url,
				UrlMeta:     urlMeta,
				PeerId:      ts.peerID,
				PeerHost:    &schedulerv1.PeerHost{},
				IsMigrating: false,
			},
			Limit: 0,
			Range: nil,
		})

	require.Nil(err, "start seed peer task")

	var success bool

loop:
	for {
		select {
		case <-r.Context.Done():
			break loop
		case <-r.Success:
			success = true
			break loop
		case <-r.Fail:
			break loop
		case p := <-r.PieceInfoChannel:
			if p.Finished {
				success = true
				break loop
			}
		case <-time.After(5 * time.Minute):
			buf := make([]byte, 16384)
			buf = buf[:runtime.Stack(buf, true)]
			fmt.Printf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
		}
	}

	require.True(success, "seed task should success")
}

func (ts *testSpec) runConductorTest(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *mockManager, urlMeta *commonv1.UrlMeta) {
	var (
		ptm       = mm.peerTaskManager
		pieceSize = ts.pieceSize
		taskID    = idgen.TaskIDV1(ts.url, urlMeta)
		output    = "../test/testdata/test.output"
	)
	defer func() {
		assert.Nil(os.Remove(output))
	}()

	peerTaskRequest := &schedulerv1.PeerTaskRequest{
		Url:      ts.url,
		UrlMeta:  urlMeta,
		PeerId:   ts.peerID,
		PeerHost: &schedulerv1.PeerHost{},
	}

	ptc, created, err := ptm.getOrCreatePeerTaskConductor(
		context.Background(), taskID, peerTaskRequest, rate.Limit(pieceSize*4), nil, nil, "", false)
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
		request := &schedulerv1.PeerTaskRequest{
			Url:      ts.url,
			UrlMeta:  urlMeta,
			PeerId:   fmt.Sprintf("should-not-use-peer-%d", i),
			PeerHost: &schedulerv1.PeerHost{},
		}
		p, created, err := ptm.getOrCreatePeerTaskConductor(
			context.Background(), taskID, request, rate.Limit(pieceSize*3), nil, nil, "", false)
		assert.Nil(err, fmt.Sprintf("load peerTaskConductor %d", i))
		assert.Equal(ptc.peerID, p.GetPeerID(), fmt.Sprintf("ptc %d should be same with ptc", i))
		assert.False(created, "should not create a new peerTaskConductor")
		go syncFunc(i, p)
	}

	require.Nil(ptc.start(), "peerTaskConductor start should be ok")

	switch ts.sizeScope {
	case commonv1.SizeScope_TINY:
		require.NotNil(ptc.tinyData)
	case commonv1.SizeScope_SMALL:
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
		ptm.runningPeerTasks.Range(func(key, value any) bool {
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
	assert.NotNil(rc, "reuse stream task")
	if rc == nil {
		return
	}

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
			PeerTaskRequest: schedulerv1.PeerTaskRequest{
				Url:      ts.url,
				UrlMeta:  urlMeta,
				PeerId:   ts.peerID,
				PeerHost: &schedulerv1.PeerHost{},
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
