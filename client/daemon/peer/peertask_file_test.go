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
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/source"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestFilePeerTask_BackSource_WithContentLength(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)

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
		url    = "http://localhost/test/data"
	)
	defer os.Remove(output)

	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
	defer storageManager.CleanUp()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
		rc := ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			))
		return rc, rc, nil
	})

	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	source.Register("http", sourceClient)
	defer source.UnRegister("http")
	sourceClient.EXPECT().GetContentLength(gomock.Any(), url, gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, headers source.RequestHeader) (int64, error) {
			return int64(len(testBytes)), nil
		})
	sourceClient.EXPECT().Download(gomock.Any(), url, gomock.Any()).DoAndReturn(
		func(ctx context.Context, url string, headers source.RequestHeader) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewBuffer(testBytes)), nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:   storageManager,
			pieceDownloader:  downloader,
			computePieceSize: computePieceSize,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	req := &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      "http://localhost/test/data",
			Filter:   "",
			BizId:    "d7y-test",
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	}
	ctx := context.Background()
	_, pt, _, err := newFilePeerTask(ctx,
		ptm.host,
		ptm.pieceManager,
		&req.PeerTaskRequest,
		ptm.schedulerClient,
		ptm.schedulerOption,
		0)
	assert.Nil(err, "new file peer task")
	pt.(*filePeerTask).backSource = true

	pt.SetCallback(&filePeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})

	progress, err := pt.Start(ctx)
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

func TestFilePeerTask_BackSource_WithoutContentLength(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)

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
		url    = "http://localhost/test/data"
	)
	defer os.Remove(output)

	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
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
	sourceClient.EXPECT().GetContentLength(gomock.Any(), url, source.RequestHeader{}).DoAndReturn(
		func(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
			return -1, nil
		})
	sourceClient.EXPECT().Download(gomock.Any(), url, source.RequestHeader{}).DoAndReturn(
		func(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewBuffer(testBytes)), nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:   storageManager,
			pieceDownloader:  downloader,
			computePieceSize: computePieceSize,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	req := &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      "http://localhost/test/data",
			Filter:   "",
			BizId:    "d7y-test",
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	}
	ctx := context.Background()
	_, pt, _, err := newFilePeerTask(ctx,
		ptm.host,
		ptm.pieceManager,
		&req.PeerTaskRequest,
		ptm.schedulerClient,
		ptm.schedulerOption,
		0)
	assert.Nil(err, "new file peer task")
	pt.(*filePeerTask).backSource = true

	pt.SetCallback(&filePeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})

	progress, err := pt.Start(ctx)
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
