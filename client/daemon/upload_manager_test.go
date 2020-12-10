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

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

type mockStorageManager struct {
	data []byte
}

func (m mockStorageManager) PutPiece(ctx context.Context, req *PutPieceRequest) error {
	panic("implement me")
}

func (m mockStorageManager) GetPiece(ctx context.Context, req *GetPieceRequest) (io.Reader, io.Closer, error) {
	return bytes.NewBuffer(m.data[req.Range.Start : req.Range.Start+req.Range.Length]), ioutil.NopCloser(nil), nil
}

func (m mockStorageManager) StoreTaskData(ctx context.Context, req *StoreRequest) error {
	panic("implement me")
}

func TestUploadManager_Serve(t *testing.T) {
	assert := testifyassert.New(t)
	testData, err := ioutil.ReadFile(testFile)
	assert.Nil(err, "load test file")

	um, err := NewUploadManager(&mockStorageManager{data: testData})
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
			fmt.Sprintf("http://%s/peer/file/%s", addr, tt.taskID), nil)
		req.Header.Add("Range", tt.pieceRange)

		resp, err := http.DefaultClient.Do(req)
		assert.Nil(err, "get piece data")

		data, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		assert.Equal(data, tt.targetPieceData)
	}
}
