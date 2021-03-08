package transport

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/daemon/test"
	mock_peer "d7y.io/dragonfly/v2/client/daemon/test/mock/peer"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func TestMain(m *testing.M) {
	logcore.InitDaemon()
	m.Run()
}

func TestTransport_RoundTrip(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	testData, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var url = "http://x/y"
	peerTaskManager := mock_peer.NewMockPeerTaskManager(ctrl)
	peerTaskManager.EXPECT().StartStreamPeerTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *scheduler.PeerTaskRequest) (io.Reader, map[string]string, error) {
			assert.Equal(req.Url, url)
			return bytes.NewBuffer(testData), nil, nil
		},
	)
	rt, _ := New(
		WithPeerHost(&scheduler.PeerHost{}),
		WithPeerTaskManager(peerTaskManager),
		WithCondition(func(r *http.Request) bool {
			return true
		}))
	assert.NotNil(rt)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := rt.RoundTrip(req)
	assert.Nil(err)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	output, err := ioutil.ReadAll(resp.Body)
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal(testData, output)
}
