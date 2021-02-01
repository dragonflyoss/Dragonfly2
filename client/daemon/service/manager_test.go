package service

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	mock_peer "github.com/dragonflyoss/Dragonfly2/client/daemon/test/mock/peer"
	mock_storage "github.com/dragonflyoss/Dragonfly2/client/daemon/test/mock/storage"
	"github.com/dragonflyoss/Dragonfly2/client/clientutil"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	dfdaemongrpc "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	dfclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/client"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

func TestMain(m *testing.M) {
	logger.InitDaemon()
	m.Run()
}

func TestDownloadManager_ServeDownload(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPeerTaskManager := mock_peer.NewMockPeerTaskManager(ctrl)
	mockPeerTaskManager.EXPECT().StartFilePeerTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *peer.FilePeerTaskRequest) (chan *peer.PeerTaskProgress, error) {
			ch := make(chan *peer.PeerTaskProgress)
			go func() {
				for i := 0; i <= 100; i++ {
					ch <- &peer.PeerTaskProgress{
						State: &base.ResponseState{
							Success: true,
							Code:    base.Code_SUCCESS,
							Msg:     "test ok",
						},
						TaskId:          "",
						PeerID:          "",
						ContentLength:   100,
						CompletedLength: int64(i),
						Done:            i == 100,
					}
				}
				close(ch)
			}()
			return ch, nil
		})
	m := &manager{
		KeepAlive:       clientutil.NewKeepAlive("test"),
		peerHost:        &scheduler.PeerHost{},
		peerTaskManager: mockPeerTaskManager,
	}
	m.downloadServer = rpc.NewServer(m)
	port, err := freeport.GetFreePort()
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(err, "get free port should be ok")
	go func() {
		m.ServeDownload(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	client, err := dfclient.CreateClient([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", port),
		},
	})
	assert.Nil(err, "grpc dial should be ok")
	request := &dfdaemongrpc.DownRequest{
		Url:    "http://localhost/test",
		Output: "./testdata/file1",
		BizId:  "unit test",
	}
	down, err := client.Download(context.Background(), request)
	assert.Nil(err, "client download grpc call should be ok")

	var result *dfdaemongrpc.DownResult
	for result = range down {
		assert.Equal(true, result.GetState().Success)
	}
	assert.NotNil(result)
	assert.True(result.Done)
}

func TestDownloadManager_ServePeer(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var maxPieceNum int32 = 10
	mockStorageManger := mock_storage.NewMockManager(ctrl)
	mockStorageManger.EXPECT().GetPieces(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, req *base.PieceTaskRequest) (*base.PiecePacket, error) {
		var (
			pieces    []*base.PieceInfo
			pieceSize = int32(1024)
		)
		for i := req.StartNum; i < req.Limit+req.StartNum && i < maxPieceNum; i++ {
			pieces = append(pieces, &base.PieceInfo{
				PieceNum:    i,
				RangeStart:  uint64(i * pieceSize),
				RangeSize:   pieceSize,
				PieceMd5:    "",
				PieceOffset: uint64(i * pieceSize),
				PieceStyle:  base.PieceStyle_PLAIN,
			})
		}
		return &base.PiecePacket{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_SUCCESS,
				Msg:     "",
			},
			TaskId:        "",
			DstPid:        "",
			DstAddr:       "",
			PieceInfos:    pieces,
			TotalPiece:    10,
			ContentLength: 10 * int64(pieceSize),
			PieceMd5Sign:  "",
		}, nil
	})
	m := &manager{
		KeepAlive:      clientutil.NewKeepAlive("test"),
		peerHost:       &scheduler.PeerHost{},
		storageManager: mockStorageManger,
	}
	m.peerServer = rpc.NewServer(m)
	port, err := freeport.GetFreePort()
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(err, "get free port should be ok")
	go func() {
		m.ServePeer(ln)
	}()
	time.Sleep(100 * time.Millisecond)

	client, err := dfclient.CreateClient([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", port),
		},
	})
	assert.Nil(err, "grpc dial should be ok")

	var tests = []struct {
		request           *base.PieceTaskRequest
		responsePieceSize int
	}{
		{
			request: &base.PieceTaskRequest{
				StartNum: 0,
				Limit:    1,
			},
			// 0
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 0,
				Limit:    4,
			},
			// 0 1 2 3
			responsePieceSize: 4,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 8,
				Limit:    1,
			},
			// 8
			responsePieceSize: 1,
		},
		{
			request: &base.PieceTaskRequest{
				StartNum: 8,
				Limit:    4,
			},
			// 8 9
			responsePieceSize: 2,
		},
	}

	for _, tc := range tests {
		response, err := client.GetPieceTasks(context.Background(), tc.request)
		assert.Nil(err, "client get piece tasks grpc call should be ok")
		assert.Equal(tc.responsePieceSize, len(response.PieceInfos))
	}
}
