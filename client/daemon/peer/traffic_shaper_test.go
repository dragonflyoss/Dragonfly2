/*
 *     Copyright 2022 The Dragonfly Authors
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
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"
	testifyrequire "github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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

type taskOption struct {
	taskID        string
	contentLength int64
	content       []byte
}

type trafficShaperComponentsOption struct {
	tasks             []taskOption
	pieceSize         uint32
	pieceDownloader   PieceDownloader
	perPeerRateLimit  rate.Limit
	totalRateLimit    rate.Limit
	trafficShaperType string
	sourceClient      source.ResourceClient
	backSource        bool
}

func trafficShaperSetupPeerTaskManagerComponents(ctrl *gomock.Controller, opt trafficShaperComponentsOption) (
	schedulerclient.V1, storage.Manager) {
	taskMap := make(map[string]*taskOption)
	piecesMap := make(map[string][]string)
	totalDigestsMap := make(map[string]string)
	for _, task := range opt.tasks {
		taskMap[task.taskID] = &task
		r := bytes.NewBuffer(task.content)
		var pieces = make([]string, int(math.Ceil(float64(len(task.content))/float64(opt.pieceSize))))
		for i := range pieces {
			pieces[i] = digest.MD5FromReader(io.LimitReader(r, int64(opt.pieceSize)))
		}
		piecesMap[task.taskID] = pieces
		totalDigestsMap[task.taskID] = digest.SHA256FromStrings(pieces...)
	}
	port := int32(freeport.GetPort())
	// 1. set up a mock daemon server for uploading pieces info
	var daemon = dfdaemonv1mocks.NewMockDaemonServer(ctrl)

	// 1.1 calculate piece digest and total digest
	genPiecePacket := func(request *commonv1.PieceTaskRequest) *commonv1.PiecePacket {
		var pieces []*commonv1.PieceInfo
		task := taskMap[request.TaskId]
		for i := uint32(0); i < request.Limit; i++ {
			start := opt.pieceSize * (request.StartNum + i)
			if int64(start)+1 > task.contentLength {
				break
			}
			size := opt.pieceSize
			if int64(start+opt.pieceSize) > task.contentLength {
				size = uint32(task.contentLength) - start
			}
			pieces = append(pieces,
				&commonv1.PieceInfo{
					PieceNum:    int32(request.StartNum + i),
					RangeStart:  uint64(start),
					RangeSize:   size,
					PieceMd5:    piecesMap[request.TaskId][request.StartNum+i],
					PieceOffset: 0,
					PieceStyle:  0,
				})
		}
		return &commonv1.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        "peer-x",
			PieceInfos:    pieces,
			ContentLength: task.contentLength,
			TotalPiece:    int32(math.Ceil(float64(task.contentLength) / float64(opt.pieceSize))),
			PieceMd5Sign:  totalDigestsMap[request.TaskId],
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
	ppsMap := make(map[string]*schedulerv1mocks.MockScheduler_ReportPieceResultClient)
	for _, task := range opt.tasks {
		pps := schedulerv1mocks.NewMockScheduler_ReportPieceResultClient(ctrl)
		pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
			func(pr *schedulerv1.PieceResult) error {
				return nil
			})
		var sent = make(chan struct{}, 1)
		sent <- struct{}{}
		pps.EXPECT().Recv().AnyTimes().DoAndReturn(
			func() (*schedulerv1.PeerPacket, error) {
				<-sent
				if opt.backSource {
					return nil, dferrors.Newf(commonv1.Code_SchedNeedBackSource, "fake back source error")
				}
				return &schedulerv1.PeerPacket{
					Code:   commonv1.Code_Success,
					TaskId: task.taskID,
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
		ppsMap[task.taskID] = pps
	}

	sched := schedulerclientmocks.NewMockV1(ctrl)
	sched.EXPECT().RegisterPeerTask(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
			return &schedulerv1.RegisterResult{
				TaskId:      ptr.TaskId,
				SizeScope:   commonv1.SizeScope_NORMAL,
				DirectPiece: nil,
			}, nil
		})
	sched.EXPECT().ReportPieceResult(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ptr *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (
			schedulerv1.Scheduler_ReportPieceResultClient, error) {
			return ppsMap[ptr.TaskId], nil
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

type trafficShaperMockManager struct {
	testSpec        *trafficShaperTestSpec
	peerTaskManager *peerTaskManager
	schedulerClient schedulerclient.V1
	storageManager  storage.Manager
}

func (m *trafficShaperMockManager) CleanUp() {
	m.storageManager.CleanUp()
	for _, f := range m.testSpec.cleanUp {
		f()
	}
}

func trafficShaperSetupMockManager(ctrl *gomock.Controller, ts *trafficShaperTestSpec, opt trafficShaperComponentsOption) *trafficShaperMockManager {
	schedulerClient, storageManager := trafficShaperSetupPeerTaskManagerComponents(ctrl, opt)
	scheduleTimeout := util.Duration{Duration: 10 * time.Minute}
	ptm := &peerTaskManager{
		conductorLock:    &sync.Mutex{},
		runningPeerTasks: sync.Map{},
		trafficShaper: NewTrafficShaper(opt.trafficShaperType, opt.totalRateLimit, func(contentLength int64) uint32 {
			return opt.pieceSize
		}),
		TaskManagerOption: TaskManagerOption{
			SchedulerClient:  schedulerClient,
			PerPeerRateLimit: opt.perPeerRateLimit,
			TaskOption: TaskOption{
				CalculateDigest: true,
				PeerHost: &schedulerv1.PeerHost{
					Ip: "127.0.0.1",
				},
				PieceManager: &pieceManager{
					Limiter:         rate.NewLimiter(opt.totalRateLimit, int(opt.totalRateLimit)),
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
	ptm.trafficShaper.Start()
	return &trafficShaperMockManager{
		testSpec:        ts,
		peerTaskManager: ptm,
		schedulerClient: schedulerClient,
		storageManager:  storageManager,
	}
}

type trafficShaperTestSpec struct {
	name                 string
	taskDelays           []time.Duration
	taskData             []byte
	pieceSize            int
	peerID               string
	url                  string
	perPeerRateLimit     rate.Limit
	totalRateLimit       rate.Limit
	backSource           bool
	mockPieceDownloader  func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader
	mockHTTPSourceClient func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient
	cleanUp              []func()
}

func TestTrafficShaper_TaskSuite(t *testing.T) {
	require := testifyrequire.New(t)
	testBytes, err := os.ReadFile(test.File)
	require.Nil(err, "load test file")

	commonPieceDownloader := func(ctrl *gomock.Controller, taskData []byte, pieceSize int) PieceDownloader {
		downloader := NewMockPieceDownloader(ctrl)
		downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
				rc := io.NopCloser(
					bytes.NewBuffer(
						taskData[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
					))
				return rc, rc, nil
			})
		return downloader
	}
	sourceClient := func(hasLength bool) func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
		return func(t *testing.T, ctrl *gomock.Controller, rg *nethttp.Range, taskData []byte, url string) source.ResourceClient {
			sourceClient := sourcemocks.NewMockResourceClient(ctrl)
			sourceClient.EXPECT().GetContentLength(gomock.Any()).AnyTimes().DoAndReturn(
				func(request *source.Request) (int64, error) {
					if hasLength {
						return int64(len(taskData)), nil
					}
					return -1, nil
				})
			sourceClient.EXPECT().Download(gomock.Any()).AnyTimes().DoAndReturn(
				func(request *source.Request) (*source.Response, error) {
					return source.NewResponse(io.NopCloser(bytes.NewBuffer(taskData))), nil
				})
			return sourceClient
		}
	}

	testCases := []trafficShaperTestSpec{
		{
			name: "normal size scope - p2p - single task",
			taskDelays: []time.Duration{
				0,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-p2p-single-task",
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name: "normal size scope - p2p - multiple tasks - concurrency",
			taskDelays: []time.Duration{
				0,
				0,
				0,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-p2p-multiple-tasks",
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name: "normal size scope - p2p - multiple tasks - overlapped",
			taskDelays: []time.Duration{
				0,
				100 * time.Millisecond,
				500 * time.Millisecond,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-p2p-multiple-tasks",
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name: "normal size scope - p2p - multiple tasks - overlapped - low bandwidth",
			taskDelays: []time.Duration{
				0,
				100 * time.Millisecond,
				500 * time.Millisecond,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-p2p-multiple-tasks",
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 2),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name: "normal size scope - p2p - multiple tasks - non-overlapped",
			taskDelays: []time.Duration{
				0,
				600 * time.Millisecond,
				1100 * time.Millisecond,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-p2p-multiple-tasks",
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  commonPieceDownloader,
			mockHTTPSourceClient: nil,
		},
		{
			name: "normal size scope - back source - single task - content length",
			taskDelays: []time.Duration{
				0,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-back-source-single-task-length",
			backSource:           true,
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: sourceClient(true),
		},
		{
			name: "normal size scope - back source - single task - no content length",
			taskDelays: []time.Duration{
				0,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-back-source-single-task-no-length",
			backSource:           true,
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: sourceClient(false),
		},
		{
			name: "normal size scope - back source - multiple tasks - content length",
			taskDelays: []time.Duration{
				0,
				100 * time.Millisecond,
				500 * time.Millisecond,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-back-source-multiple-tasks-length",
			backSource:           true,
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: sourceClient(true),
		},
		{
			name: "normal size scope - back source - multiple tasks - no content length",
			taskDelays: []time.Duration{
				0,
				100 * time.Millisecond,
				500 * time.Millisecond,
			},
			taskData:             testBytes,
			pieceSize:            1024,
			peerID:               "normal-size-peer-back-source-multiple-tasks-no-length",
			backSource:           true,
			url:                  "http://localhost/test/data",
			perPeerRateLimit:     rate.Limit(1024 * 4),
			totalRateLimit:       rate.Limit(1024 * 10),
			mockPieceDownloader:  nil,
			mockHTTPSourceClient: sourceClient(false),
		},
	}

	for _, _tc := range testCases {
		t.Run(_tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			require := testifyrequire.New(t)
			for _, trafficShaperType := range []string{"plain", "sampling"} {
				// dup a new test case with the task type
				logger.Infof("-------------------- test %s, %s traffic shaper started --------------------",
					_tc.name, trafficShaperType)
				tc := _tc
				func() {
					tasks := make([]taskOption, 0)
					ctrl := gomock.NewController(t)
					defer ctrl.Finish()
					mockContentLength := len(tc.taskData)
					urlMetas := make([]*commonv1.UrlMeta, len(tc.taskDelays))
					for i := range tc.taskDelays {
						urlMeta := &commonv1.UrlMeta{
							Tag: "d7y-test",
						}
						urlMetas[i] = urlMeta

						taskID := idgen.TaskIDV1(tc.url+fmt.Sprintf("-%d", i), urlMeta)
						tasks = append(tasks, taskOption{
							taskID:        taskID,
							contentLength: int64(mockContentLength),
							content:       tc.taskData,
						})
					}

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
						sourceClient = tc.mockHTTPSourceClient(t, ctrl, nil, tc.taskData, tc.url)
						require.Nil(source.Register("http", sourceClient, httpprotocol.Adapter))
					}

					option := trafficShaperComponentsOption{
						tasks:             tasks,
						pieceSize:         uint32(tc.pieceSize),
						pieceDownloader:   downloader,
						perPeerRateLimit:  tc.perPeerRateLimit,
						totalRateLimit:    tc.totalRateLimit,
						trafficShaperType: trafficShaperType,
						sourceClient:      sourceClient,
						backSource:        tc.backSource,
					}
					mm := trafficShaperSetupMockManager(ctrl, &tc, option)
					defer mm.CleanUp()
					var start = time.Now()
					tc.run(assert, require, mm, urlMetas)
					elapsed := time.Since(start)
					logger.Infof("test: %s, traffic shaper: %s finished, took %s",
						_tc.name, trafficShaperType, elapsed)
				}()
				logger.Infof("-------------------- test %s, %s traffic shaper, finished --------------------",
					_tc.name, trafficShaperType)
			}
		})
	}
}

func (ts *trafficShaperTestSpec) run(assert *testifyassert.Assertions, require *testifyrequire.Assertions, mm *trafficShaperMockManager, urlMetas []*commonv1.UrlMeta) {
	var (
		ptm      = mm.peerTaskManager
		ptcCount = len(ts.taskDelays)
	)

	ptcs := make([]*peerTaskConductor, ptcCount)

	for i := range ts.taskDelays {
		taskID := idgen.TaskIDV1(ts.url+fmt.Sprintf("-%d", i), urlMetas[i])
		peerTaskRequest := &schedulerv1.PeerTaskRequest{
			Url:      ts.url + fmt.Sprintf("-%d", i),
			UrlMeta:  urlMetas[i],
			PeerId:   ts.peerID,
			PeerHost: &schedulerv1.PeerHost{},
		}
		logger.Infof("taskID: %s", taskID)
		ptc, created, err := ptm.getOrCreatePeerTaskConductor(
			context.Background(), taskID, peerTaskRequest, ts.perPeerRateLimit, nil, nil, "", false)
		assert.Nil(err, "load first peerTaskConductor")
		assert.True(created, "should create a new peerTaskConductor")
		ptcs[i] = ptc
	}

	var wg = &sync.WaitGroup{}
	wg.Add(ptcCount)

	var result = make([]bool, ptcCount)

	for i, ptc := range ptcs {
		go func(ptc *peerTaskConductor, i int) {
			time.Sleep(ts.taskDelays[i])
			require.Nil(ptc.start(), "peerTaskConductor start should be ok")
			start := time.Now()
			defer wg.Done()
			select {
			case <-time.After(5 * time.Minute):
				ptc.Fail()
			case <-ptc.successCh:
				elapsed := time.Since(start)
				logger.Infof("%dth task %s succeed, took %s", i, ptc.taskID, elapsed)
				result[i] = true
				return
			case <-ptc.failCh:
				return
			}
		}(ptc, i)
	}

	wg.Wait()

	for i, r := range result {
		assert.True(r, fmt.Sprintf("task %d result should be true", i))
	}

	for _, ptc := range ptcs {
		var success bool
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
	}
}
