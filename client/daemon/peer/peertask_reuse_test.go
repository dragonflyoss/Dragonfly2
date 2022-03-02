/*
 *     Copyright 2022 The Dragonfly Authors
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
	"os"
	"path"
	"testing"

	"github.com/go-http-utils/headers"
	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/test"
	ms "d7y.io/dragonfly/v2/client/daemon/test/mock/storage"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func TestReuseFilePeerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	assert := testifyassert.New(t)

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err)
	testOutput := path.Join(os.TempDir(), "d7y-reuse-output.data")
	defer os.Remove(testOutput)

	var testCases = []struct {
		name           string
		request        *FileTaskRequest
		enablePrefetch bool
		storageManager func(sm *ms.MockManager)
		verify         func(pg chan *FileTaskProgress, ok bool)
	}{
		{
			name: "normal completed task found",
			request: &FileTaskRequest{
				PeerTaskRequest: scheduler.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &base.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  nil,
			},
			enablePrefetch: false,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 10,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().Store(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.StoreRequest) error {
						return os.WriteFile(req.Destination, testBytes[0:10], 0644)
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.True(ok)
				data, err := os.ReadFile(testOutput)
				assert.Nil(err)
				assert.Equal(testBytes[0:10], data)
			},
		},
		{
			name: "normal completed task not found",
			request: &FileTaskRequest{
				PeerTaskRequest: scheduler.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &base.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  nil,
			},
			enablePrefetch: false,
			storageManager: func(sm *ms.MockManager) {
				//sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
				//	func(taskID string, rg *clientutil.Range) *storage.ReusePeerTask {
				//		return nil
				//	})
				//sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
				//	func(taskID string) *storage.ReusePeerTask {
				//		return nil
				//	})
				sm.EXPECT().FindCompletedTask(gomock.Any()).DoAndReturn(
					func(taskID string) *storage.ReusePeerTask {
						return nil
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.False(ok)
				assert.Nil(pg)
			},
		},
		{
			name: "normal completed subtask found",
			request: &FileTaskRequest{
				PeerTaskRequest: scheduler.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &base.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &clientutil.Range{Start: 200, Length: 100},
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 10,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().Store(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.StoreRequest) error {
						return os.WriteFile(req.Destination, testBytes[200:300], 0644)
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.True(ok)
				data, err := os.ReadFile(testOutput)
				assert.Nil(err)
				assert.Equal(testBytes[200:300], data)
			},
		},
		{
			name: "normal completed subtask not found",
			request: &FileTaskRequest{
				PeerTaskRequest: scheduler.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &base.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &clientutil.Range{Start: 0, Length: 10},
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(taskID string, rg *clientutil.Range) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(taskID string) *storage.ReusePeerTask {
						return nil
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.False(ok)
				assert.Nil(pg)
			},
		},
		{
			name: "partial task found",
			request: &FileTaskRequest{
				PeerTaskRequest: scheduler.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &base.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &clientutil.Range{Start: 300, Length: 100},
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *clientutil.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 100,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer(testBytes[300:400])), nil
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.True(ok)
				data, err := os.ReadFile(testOutput)
				assert.Nil(err)
				assert.Equal(testBytes[300:400], data)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer os.Remove(testOutput)
			sm := ms.NewMockManager(ctrl)
			tc.storageManager(sm)
			ptm := &peerTaskManager{
				host:           &scheduler.PeerHost{},
				enablePrefetch: tc.enablePrefetch,
				storageManager: sm,
			}
			tc.verify(ptm.tryReuseFilePeerTask(context.Background(), tc.request))
		})
	}
}

func TestReuseStreamPeerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	assert := testifyassert.New(t)

	var testCases = []struct {
		name           string
		request        *StreamTaskRequest
		enablePrefetch bool
		storageManager func(sm *ms.MockManager)
		verify         func(rc io.ReadCloser, attr map[string]string, ok bool)
	}{
		{
			name: "normal completed task found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &base.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  nil,
				PeerID: "",
			},
			enablePrefetch: false,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 10,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer([]byte("1111111111"))), nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				_, exist := attr[headers.ContentRange]
				assert.False(exist)
			},
		},
		{
			name: "normal completed task not found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &base.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  nil,
				PeerID: "",
			},
			enablePrefetch: false,
			storageManager: func(sm *ms.MockManager) {
				//sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
				//	func(taskID string, rg *clientutil.Range) *storage.ReusePeerTask {
				//		return nil
				//	})
				//sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
				//	func(taskID string) *storage.ReusePeerTask {
				//		return nil
				//	})
				sm.EXPECT().FindCompletedTask(gomock.Any()).DoAndReturn(
					func(taskID string) *storage.ReusePeerTask {
						return nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.False(ok)
				assert.Nil(rc)
				assert.Nil(attr)
			},
		},
		{
			name: "normal completed subtask found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &base.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &clientutil.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 10,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer([]byte("1111111111"))), nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				assert.Equal("bytes 0-9/*", attr[headers.ContentRange])
			},
		},
		{
			name: "normal completed subtask not found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &base.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &clientutil.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(taskID string, rg *clientutil.Range) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(taskID string) *storage.ReusePeerTask {
						return nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.False(ok)
				assert.Nil(rc)
				assert.Nil(attr)
			},
		},
		{
			name: "partial task found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &base.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &clientutil.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *ms.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *clientutil.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: 100,
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer([]byte("1111111111"))), nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				assert.Equal("bytes 0-9/100", attr[headers.ContentRange])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := ms.NewMockManager(ctrl)
			tc.storageManager(sm)
			ptm := &peerTaskManager{
				host:           &scheduler.PeerHost{},
				enablePrefetch: tc.enablePrefetch,
				storageManager: sm,
			}
			tc.verify(ptm.tryReuseStreamPeerTask(context.Background(), tc.request))
		})
	}
}
