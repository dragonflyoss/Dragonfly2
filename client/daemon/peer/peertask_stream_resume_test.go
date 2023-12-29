/*
 *     Copyright 2023 The Dragonfly Authors
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
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-http-utils/headers"
	testifyassert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	schedulerv1mocks "d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/net/http"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	clientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/clients/httpprotocol"
	sourcemocks "d7y.io/dragonfly/v2/pkg/source/mocks"
)

func setupResumeStreamTaskComponents(ctrl *gomock.Controller, opt componentsOption) (
	schedulerclient.V1, storage.Manager) {
	// set up a scheduler to say back source only
	pps := schedulerv1mocks.NewMockScheduler_ReportPieceResultClient(ctrl)
	pps.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(
		func(pr *schedulerv1.PieceResult) error {
			return nil
		})
	pps.EXPECT().Recv().AnyTimes().DoAndReturn(
		func() (*schedulerv1.PeerPacket, error) {
			return nil, dferrors.New(commonv1.Code_SchedNeedBackSource, "")
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

	// set up storage manager
	tempDir, _ := os.MkdirTemp("", "d7y-test-*")
	storageManager, _ := storage.NewStorageManager(
		config.SimpleLocalTaskStoreStrategy,
		&config.StorageOption{
			DataPath: tempDir,
			TaskExpireTime: util.Duration{
				Duration: -1 * time.Second,
			},
		}, func(request storage.CommonTaskRequest) {},
		os.FileMode(0700))
	return sched, storageManager
}

type intervalSleepReader struct {
	offset   int
	size     int
	data     []byte
	interval time.Duration
}

func (i *intervalSleepReader) Read(p []byte) (n int, err error) {
	if i.offset >= len(i.data) {
		return 0, io.EOF
	}
	end := i.offset + i.size
	if end > len(i.data) {
		end = len(i.data)
	}

	n = copy(p, i.data[i.offset:end])
	time.Sleep(i.interval)

	i.offset += n
	if i.offset >= len(i.data) {
		return n, io.EOF
	}
	return n, nil
}

func (i *intervalSleepReader) Close() error {
	return nil
}

func TestStreamPeerTask_Resume(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var (
		pieceParallelCount = int32(4)
		pieceSize          = 1024

		pieceDownloadInterval = time.Millisecond * 100

		mockContentLength = len(testBytes)
		//mockPieceCount    = int(math.Ceil(float64(mockContentLength) / float64(pieceSize)))

		peerID = "peer-resume-0"
		taskID = "task-resume-0"

		url = "http://localhost/test/data"
	)

	schedulerClient, storageManager := setupResumeStreamTaskComponents(
		ctrl,
		componentsOption{
			taskID:             taskID,
			contentLength:      int64(mockContentLength),
			pieceSize:          uint32(pieceSize),
			pieceParallelCount: pieceParallelCount,
			content:            testBytes,
		})
	defer storageManager.CleanUp()

	sourceClient := sourcemocks.NewMockResourceClient(ctrl)
	source.UnRegister("http")
	require.Nil(t, source.Register("http", sourceClient, httpprotocol.Adapter))
	defer source.UnRegister("http")
	sourceClient.EXPECT().Download(gomock.Any()).DoAndReturn(
		func(request *source.Request) (*source.Response, error) {
			response := source.NewResponse(
				&intervalSleepReader{
					size:     pieceSize,
					data:     testBytes,
					interval: pieceDownloadInterval,
				})
			response.ContentLength = int64(len(testBytes))
			return response, nil
		})

	pm := &pieceManager{
		calculateDigest: true,
		pieceDownloader: nil,
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
	wg := &sync.WaitGroup{}

	// set up parent task
	wg.Add(1)

	pt, err := ptm.newStreamTask(ctx, taskID, req, nil)
	assert.Nil(err, "new parent stream peer task")

	rc, _, err := pt.Start(ctx)
	assert.Nil(err, "start parent stream peer task")

	ptc := pt.peerTaskConductor

	go func() {
		outputBytes, err := io.ReadAll(rc)
		assert.Nil(err, "load read data")
		assert.Equal(testBytes, outputBytes, "output and desired output must match")
		wg.Done()
	}()

	ranges := []*http.Range{
		{
			Start:  0,
			Length: int64(mockContentLength),
		},
		{
			Start:  10,
			Length: int64(mockContentLength) - 10,
		},
		{
			Start:  100,
			Length: int64(mockContentLength) - 100,
		},
		{
			Start:  1000,
			Length: int64(mockContentLength) - 1000,
		},
		{
			Start:  1024,
			Length: int64(mockContentLength) - 1024,
		},
	}

	wg.Add(len(ranges))
	for _, rg := range ranges {
		go func(rg *http.Range) {
			pt := ptm.newResumeStreamTask(ctx, ptc, rg)
			assert.NotNil(pt, "new stream peer task")

			pt.computePieceSize = func(length int64) uint32 {
				return uint32(pieceSize)
			}

			rc, attr, err := pt.Start(ctx)
			assert.Nil(err, "start stream peer task")

			assert.Equal(attr[headers.ContentLength], fmt.Sprintf("%d", rg.Length), "content length should match")
			assert.Equal(attr[headers.ContentRange], fmt.Sprintf("bytes %d-%d/%d", rg.Start, mockContentLength-1, mockContentLength), "content length should match")

			outputBytes, err := io.ReadAll(rc)
			assert.Nil(err, "load read data")
			assert.Equal(len(testBytes[rg.Start:rg.Start+rg.Length]), len(outputBytes), "output and desired output length must match")
			assert.Equal(string(testBytes[rg.Start:rg.Start+rg.Length]), string(outputBytes), "output and desired output must match")
			wg.Done()
		}(rg)
	}

	wg.Wait()
}
