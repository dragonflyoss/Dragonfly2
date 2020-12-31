package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/phayes/freeport"
	testifyassert "github.com/stretchr/testify/assert"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	mock_peer "github.com/dragonflyoss/Dragonfly2/client/daemon/test/mock/peer"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	dfdaemongrpc "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	dfclient "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/client"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

func TestDownloadManager_ServeGRPC(t *testing.T) {
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
						CompletedLength: uint64(i),
						Done:            i == 100,
					}
				}
				close(ch)
			}()
			return ch, nil
		})
	dm := rpcManager{
		peerHost:        &scheduler.PeerHost{},
		peerTaskManager: mockPeerTaskManager,
	}
	port, err := freeport.GetFreePort()
	assert.Nil(err, "get free port should be ok")
	go func() {
		dm.ServeDaemon("tcp", fmt.Sprintf(":%d", port))
	}()
	time.Sleep(time.Second)

	client, err := dfclient.CreateClient([]basic.NetAddr{
		{
			Type: basic.TCP,
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
		assert.Equal(result.GetState().Success, true)
	}
	assert.NotNil(result)
	assert.True(result.Done)
}
