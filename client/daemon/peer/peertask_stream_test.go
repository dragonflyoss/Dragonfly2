package peer

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/client/daemon/test/mock/source"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func TestStreamPeerTask_BackSource_WithContentLength(t *testing.T) {
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

		url = "http://localhost/test/data"
	)
	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
	defer storageManager.(gc.GC).TryGC()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
		rc := ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			))
		return rc, rc, nil
	})

	sourceClient := source.NewMockResourceClient(ctrl)
	sourceClient.EXPECT().GetContentLength(url, map[string]string{}).DoAndReturn(
		func(url string, headers map[string]string) (int64, error) {
			return int64(len(testBytes)), nil
		})
	sourceClient.EXPECT().Download(url, map[string]string{}).DoAndReturn(
		func(url string, headers map[string]string) (*types.DownloadResponse, error) {
			return &types.DownloadResponse{
				Body: ioutil.NopCloser(bytes.NewBuffer(testBytes)),
			}, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager: storageManager,
		scheduler:      schedulerClient,
	}
	req := &scheduler.PeerTaskRequest{
		Url:      url,
		Filter:   "",
		BizId:    "d7y-test",
		UrlMata:  nil,
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	}
	ctx := context.Background()
	pt, err := NewStreamPeerTask(ctx,
		ptm.host,
		schedulerClient,
		&pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
			resourceClient:  sourceClient,
			computePieceSize: func(contentLength int64) int32 {
				return int32(pieceSize)
			},
		},
		req,
		time.Second)
	assert.Nil(err, "new stream peer task")
	pt.SetCallback(&streamPeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})
	pt.(*streamPeerTask).base.backSource = true

	r, _, err := pt.Start(ctx)
	assert.Nil(err, "start stream peer task")

	outputBytes, err := ioutil.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}

func TestStreamPeerTask_BackSource_WithoutContentLength(t *testing.T) {
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

		url = "http://localhost/test/data"
	)
	schedulerClient, storageManager := setupPeerTaskManagerComponents(ctrl, taskID, int64(mockContentLength), int32(pieceSize), pieceParallelCount)
	defer storageManager.(gc.GC).TryGC()

	downloader := NewMockPieceDownloader(ctrl)
	downloader.EXPECT().DownloadPiece(gomock.Any()).AnyTimes().DoAndReturn(func(task *DownloadPieceRequest) (io.Reader, io.Closer, error) {
		rc := ioutil.NopCloser(
			bytes.NewBuffer(
				testBytes[task.piece.RangeStart : task.piece.RangeStart+uint64(task.piece.RangeSize)],
			))
		return rc, rc, nil
	})

	sourceClient := source.NewMockResourceClient(ctrl)
	sourceClient.EXPECT().GetContentLength(url, map[string]string{}).DoAndReturn(
		func(url string, headers map[string]string) (int64, error) {
			return -1, nil
		})
	sourceClient.EXPECT().Download(url, map[string]string{}).DoAndReturn(
		func(url string, headers map[string]string) (*types.DownloadResponse, error) {
			return &types.DownloadResponse{
				Body: ioutil.NopCloser(bytes.NewBuffer(testBytes)),
			}, nil
		})

	ptm := &peerTaskManager{
		host: &scheduler.PeerHost{
			Ip: "127.0.0.1",
		},
		runningPeerTasks: sync.Map{},
		pieceManager: &pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
		},
		storageManager: storageManager,
		scheduler:      schedulerClient,
	}
	req := &scheduler.PeerTaskRequest{
		Url:      url,
		Filter:   "",
		BizId:    "d7y-test",
		UrlMata:  nil,
		PeerId:   peerID,
		PeerHost: &scheduler.PeerHost{},
	}
	ctx := context.Background()
	pt, err := NewStreamPeerTask(ctx,
		ptm.host,
		schedulerClient,
		&pieceManager{
			storageManager:  storageManager,
			pieceDownloader: downloader,
			resourceClient:  sourceClient,
			computePieceSize: func(contentLength int64) int32 {
				return int32(pieceSize)
			},
		},
		req,
		time.Second)
	assert.Nil(err, "new stream peer task")
	pt.SetCallback(&streamPeerTaskCallback{
		ctx:   ctx,
		ptm:   ptm,
		req:   req,
		start: time.Now(),
	})
	pt.(*streamPeerTask).base.backSource = true

	r, _, err := pt.Start(ctx)
	assert.Nil(err, "start stream peer task")

	outputBytes, err := ioutil.ReadAll(r)
	assert.Nil(err, "load read data")
	assert.Equal(testBytes, outputBytes, "output and desired output must match")
}
