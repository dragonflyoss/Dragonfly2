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

package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/storage/mocks"
	"d7y.io/dragonfly/v2/client/daemon/test"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
)

func TestUploadManager_Serve(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := testifyassert.New(t)
	testData, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")

	mockStorageManager := mocks.NewMockManager(ctrl)
	mockStorageManager.EXPECT().ReadPiece(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error) {
			return bytes.NewBuffer(testData[req.Range.Start : req.Range.Start+req.Range.Length]),
				io.NopCloser(nil), nil
		})

	um, err := NewUploadManager(config.NewDaemonConfig(), mockStorageManager, os.TempDir(), WithLimiter(rate.NewLimiter(16*1024, 16*1024)))
	assert.Nil(err, "NewUploadManager")

	listen, err := net.Listen("tcp4", "127.0.0.1:0")
	assert.Nil(err, "Listen")
	addr := listen.Addr().String()

	go func() {
		if err := um.Serve(listen); err != nil {
			t.Error(err)
		}
	}()

	tests := []struct {
		taskID          string
		peerID          string
		pieceRange      string
		targetPieceData []byte
	}{
		{
			taskID:          "task-0",
			peerID:          "peer-0",
			pieceRange:      "bytes=0-9",
			targetPieceData: testData[0:10],
		},
		{
			taskID:          "task-1",
			peerID:          "peer-1",
			pieceRange:      fmt.Sprintf("bytes=512-%d", len(testData)-1),
			targetPieceData: testData[512:],
		},
		{
			taskID:          "task-2",
			peerID:          "peer-2",
			pieceRange:      "bytes=512-1023",
			targetPieceData: testData[512:1024],
		},
	}

	for _, tt := range tests {
		req, _ := http.NewRequest(http.MethodGet,
			fmt.Sprintf("http://%s/%s/%s/%s?peerId=%s", addr, "download", "666", tt.taskID, tt.peerID), nil)
		req.Header.Add("Range", tt.pieceRange)

		resp, err := http.DefaultClient.Do(req)
		assert.Nil(err, "get piece data")

		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		assert.Equal(tt.targetPieceData, data)
	}
}
