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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"
	testifyrequire "github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourceMock "d7y.io/dragonfly/v2/pkg/source/mock"
)

func TestFilePeerTask_BackSource_WithContentLength(t *testing.T) {
	assert := testifyassert.New(t)
	require := testifyrequire.New(t)
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
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
		rc := ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			))
		return rc, rc, nil
	})

	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	source.UnRegister("http")
	require.Nil(source.Register("http", sourceClient, httpprotocol.Adapter))
	defer source.UnRegister("http")
	sourceClient.EXPECT().GetContentLength(gomock.Any()).DoAndReturn(
		func(request *source.Request) (int64, error) {
			if request.URL.String() == url {
				return int64(len(testBytes)), nil
			}
			return -1, fmt.Errorf("unexpect url: %s", request.URL.String())
		})
	sourceClient.EXPECT().Download(gomock.Any()).DoAndReturn(
		func(request *source.Request) (io.ReadCloser, error) {
			if request.URL.String() == url {
				return ioutil.NopCloser(bytes.NewBuffer(testBytes)), nil
			}
			return nil, fmt.Errorf("unexpect url: %s", request.URL.String())
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:   storageManager,
			pieceDownloader:  downloader,
			computePieceSize: util.ComputePieceSize,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	req := &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url: "http://localhost/test/data",
			UrlMeta: &base.UrlMeta{
				Tag: "d7y-test",
			},
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	}
	ctx := context.Background()
	_, pt, _, err := newFilePeerTask(ctx,
		ptm.host,
		ptm.pieceManager,
		req,
		ptm.schedulerClient,
		ptm.schedulerOption,
		0)
	assert.Nil(err, "new file peer task")
	pt.needBackSource = true

	pt.SetCallback(&filePeerTaskCallback{
		ptm:   ptm,
		pt:    pt,
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
	require := testifyrequire.New(t)
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
	downloader.EXPECT().DownloadPiece(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
			rc := ioutil.NopCloser(
				bytes.NewBuffer(
					testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
				))
			return rc, rc, nil
		})

	sourceClient := sourceMock.NewMockResourceClient(ctrl)
	require.Nil(source.Register("http", sourceClient, httpprotocol.Adapter))
	defer source.UnRegister("http")
	sourceClient.EXPECT().GetContentLength(gomock.Any()).DoAndReturn(
		func(request *source.Request) (int64, error) {
			if request.URL.String() == url {
				return -1, nil
			}
			return -1, fmt.Errorf("unexpect url: %s", request.URL.String())
		})
	sourceClient.EXPECT().Download(gomock.Any()).DoAndReturn(
		func(request *source.Request) (io.ReadCloser, error) {
			if request.URL.String() == url {
				return ioutil.NopCloser(bytes.NewBuffer(testBytes)), nil
			}
			return nil, fmt.Errorf("unexpect url: %s", request.URL.String())
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:   storageManager,
			pieceDownloader:  downloader,
			computePieceSize: util.ComputePieceSize,
		},
		storageManager:  storageManager,
		schedulerClient: schedulerClient,
		schedulerOption: config.SchedulerOption{
			ScheduleTimeout: clientutil.Duration{Duration: 10 * time.Minute},
		},
	}
	req := &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url: "http://localhost/test/data",
			UrlMeta: &base.UrlMeta{
				Tag: "d7y-test",
			},
			PeerId:   peerID,
			PeerHost: &scheduler.PeerHost{},
		},
		Output: output,
	}
	ctx := context.Background()
	_, pt, _, err := newFilePeerTask(ctx,
		ptm.host,
		ptm.pieceManager,
		req,
		ptm.schedulerClient,
		ptm.schedulerOption,
		0)
	assert.Nil(err, "new file peer task")
	pt.needBackSource = true

	pt.SetCallback(&filePeerTaskCallback{
		ptm:   ptm,
		pt:    pt,
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
