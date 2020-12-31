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
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/test"
	mock_storage "github.com/dragonflyoss/Dragonfly2/client/daemon/test/mock/storage"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
)

func TestUploadManager_Serve(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := testifyassert.New(t)
	testData, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	mockStorageManager := mock_storage.NewMockManager(ctrl)
	mockStorageManager.EXPECT().ReadPiece(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, req *storage.ReadPieceRequest) (io.Reader, io.Closer, error) {
			return bytes.NewBuffer(testData[req.Range.Start : req.Range.Start+req.Range.Length]),
				ioutil.NopCloser(nil), nil
		})

	um, err := NewUploadManager(mockStorageManager, WithLimiter(rate.NewLimiter(16*1024, 16*1024)))
	assert.Nil(err, "NewUploadManager")

	listen, err := net.Listen("tcp4", "127.0.0.1:0")
	assert.Nil(err, "Listen")
	addr := listen.Addr().String()

	go func() {
		um.Serve(listen)
	}()

	tests := []struct {
		taskID          string
		pieceRange      string
		targetPieceData []byte
	}{
		{
			taskID:          "task-0",
			pieceRange:      "bytes=0-9",
			targetPieceData: testData[0:10],
		},
		{
			taskID:          "task-1",
			pieceRange:      fmt.Sprintf("bytes=512-%d", len(testData)-1),
			targetPieceData: testData[512:],
		},
		{
			taskID:          "task-2",
			pieceRange:      "bytes=512-1023",
			targetPieceData: testData[512:1024],
		},
	}

	for _, tt := range tests {
		req, _ := http.NewRequest(http.MethodGet,
			fmt.Sprintf("http://%s%s%s/%s", addr, PeerDownloadHTTPPathPrefix, "666", tt.taskID), nil)
		req.Header.Add("Range", tt.pieceRange)

		resp, err := http.DefaultClient.Do(req)
		assert.Nil(err, "get piece data")

		data, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		assert.Equal(data, tt.targetPieceData)
	}
}
