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

package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/daemon/test"
	mock_peer "d7y.io/dragonfly/v2/client/daemon/test/mock/peer"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestTransport_RoundTrip(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	testData, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var url = "http://x/y"
	peerTaskManager := mock_peer.NewMockTaskManager(ctrl)
	peerTaskManager.EXPECT().StartStreamPeerTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *scheduler.PeerTaskRequest) (io.ReadCloser, map[string]string, error) {
			assert.Equal(req.Url, url)
			return io.NopCloser(bytes.NewBuffer(testData)), nil, nil
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
	output, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal(testData, output)
}
