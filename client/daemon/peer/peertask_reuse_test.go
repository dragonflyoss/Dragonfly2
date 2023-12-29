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
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-http-utils/headers"
	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/storage/mocks"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
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
		storageManager func(sm *mocks.MockManager)
		verify         func(pg chan *FileTaskProgress, ok bool)
	}{
		{
			name: "normal completed task found",
			request: &FileTaskRequest{
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
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
			storageManager: func(sm *mocks.MockManager) {
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
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
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
			storageManager: func(sm *mocks.MockManager) {
				//sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
				//	func(taskID string, rg *http.Range) *storage.ReusePeerTask {
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
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &http.Range{Start: 200, Length: 100},
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
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
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &http.Range{Start: 0, Length: 10},
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(taskID string, rg *http.Range) *storage.ReusePeerTask {
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
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &http.Range{Start: 300, Length: 100},
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *http.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: int64(len(testBytes)),
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer(testBytes[req.Range.Start : req.Range.Start+req.Range.Length])), nil
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.True(ok)
				data, err := os.ReadFile(testOutput)
				assert.Nil(err)
				assert.Equal(testBytes[300:400], data)
			},
		},
		{
			name: "partial task found - out of range",
			request: &FileTaskRequest{
				PeerTaskRequest: schedulerv1.PeerTaskRequest{
					PeerId: "",
					Url:    "http://example.com/1",
					UrlMeta: &commonv1.UrlMeta{
						Digest: "",
						Tag:    "",
						Range:  "",
						Filter: "",
						Header: nil,
					},
				},
				Output: testOutput,
				Range:  &http.Range{Start: 300, Length: 100000},
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *http.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: int64(len(testBytes)),
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer(testBytes[req.Range.Start : req.Range.Start+req.Range.Length])), nil
					})
			},
			verify: func(pg chan *FileTaskProgress, ok bool) {
				assert.True(ok)
				data, err := os.ReadFile(testOutput)
				assert.Nil(err)
				assert.Equal(testBytes[300:], data)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer os.Remove(testOutput)
			sm := mocks.NewMockManager(ctrl)
			tc.storageManager(sm)
			ptm := &peerTaskManager{
				TaskManagerOption: TaskManagerOption{
					TaskOption: TaskOption{
						PeerHost:        &schedulerv1.PeerHost{},
						StorageManager:  sm,
						GRPCDialTimeout: time.Second,
						GRPCCredentials: insecure.NewCredentials(),
					},
					Prefetch: tc.enablePrefetch,
				},
			}
			tc.verify(ptm.tryReuseFilePeerTask(context.Background(), tc.request))
		})
	}
}

func TestReuseStreamPeerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	assert := testifyassert.New(t)

	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err)

	var testCases = []struct {
		name           string
		request        *StreamTaskRequest
		enablePrefetch bool
		storageManager func(sm *mocks.MockManager)
		verify         func(rc io.ReadCloser, attr map[string]string, ok bool)
	}{
		{
			name: "normal completed task found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &commonv1.UrlMeta{
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
			storageManager: func(sm *mocks.MockManager) {
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
				sm.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				_, exist := attr[headers.ContentRange]
				assert.False(exist)
				rc.Close()
			},
		},
		{
			name: "normal completed task not found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &commonv1.UrlMeta{
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
			storageManager: func(sm *mocks.MockManager) {
				//sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
				//	func(taskID string, rg *http.Range) *storage.ReusePeerTask {
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
				URLMeta: &commonv1.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &http.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
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
				sm.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				assert.Equal("bytes 0-9/*", attr[headers.ContentRange])
				rc.Close()
			},
		},
		{
			name: "normal completed subtask not found",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &commonv1.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &http.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(taskID string, rg *http.Range) *storage.ReusePeerTask {
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
				URLMeta: &commonv1.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &http.Range{Start: 0, Length: 10},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *http.Range) *storage.ReusePeerTask {
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
				sm.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				assert.Equal("10", attr[headers.ContentLength])
				assert.Equal("bytes 0-9/100", attr[headers.ContentRange])
				rc.Close()
			},
		},
		{
			name: "partial task found - 2",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &commonv1.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &http.Range{Start: 0, Length: 100},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *http.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: int64(len(testBytes)),
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer(testBytes[req.Range.Start : req.Range.Start+req.Range.Length])), nil
					})
				sm.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				data, err := io.ReadAll(rc)
				assert.Nil(err)
				assert.Equal(testBytes[0:100], data)
				assert.Equal("test", attr["Test"])
				assert.Equal("100", attr[headers.ContentLength])
				assert.Equal(fmt.Sprintf("bytes 0-99/%d", len(testBytes)), attr[headers.ContentRange])
				rc.Close()
			},
		},
		{
			name: "partial task found - out of range",
			request: &StreamTaskRequest{
				URL: "http://example.com/1",
				URLMeta: &commonv1.UrlMeta{
					Digest: "",
					Tag:    "",
					Range:  "",
					Filter: "",
					Header: nil,
				},
				Range:  &http.Range{Start: 100, Length: 100000},
				PeerID: "",
			},
			enablePrefetch: true,
			storageManager: func(sm *mocks.MockManager) {
				var taskID string
				sm.EXPECT().FindCompletedSubTask(gomock.Any()).DoAndReturn(
					func(id string) *storage.ReusePeerTask {
						return nil
					})
				sm.EXPECT().FindPartialCompletedTask(gomock.Any(), gomock.Any()).DoAndReturn(
					func(id string, rg *http.Range) *storage.ReusePeerTask {
						taskID = id
						return &storage.ReusePeerTask{
							PeerTaskMetadata: storage.PeerTaskMetadata{
								TaskID: taskID,
							},
							ContentLength: int64(len(testBytes)),
							TotalPieces:   0,
							PieceMd5Sign:  "",
						}
					})
				sm.EXPECT().ReadAllPieces(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, req *storage.ReadAllPiecesRequest) (io.ReadCloser, error) {
						assert.Equal(taskID, req.TaskID)
						return io.NopCloser(bytes.NewBuffer(testBytes[req.Range.Start : req.Range.Start+req.Range.Length])), nil
					})
				sm.EXPECT().GetExtendAttribute(gomock.Any(),
					gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, req *storage.PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
						return &commonv1.ExtendAttribute{
							Header: map[string]string{
								"Test": "test",
							},
						}, nil
					})
			},
			verify: func(rc io.ReadCloser, attr map[string]string, ok bool) {
				assert.True(ok)
				assert.NotNil(rc)
				data, err := io.ReadAll(rc)
				assert.Nil(err)
				assert.Equal(testBytes[100:], data)
				assert.Equal("test", attr["Test"])
				assert.Equal(fmt.Sprintf("%d", len(testBytes)-100), attr[headers.ContentLength])
				assert.Equal(fmt.Sprintf("bytes 100-%d/%d", len(testBytes)-1, len(testBytes)), attr[headers.ContentRange])
				rc.Close()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sm := mocks.NewMockManager(ctrl)
			tc.storageManager(sm)
			ptm := &peerTaskManager{
				TaskManagerOption: TaskManagerOption{
					Prefetch: tc.enablePrefetch,
					TaskOption: TaskOption{
						PeerHost:        &schedulerv1.PeerHost{},
						StorageManager:  sm,
						GRPCDialTimeout: time.Second,
						GRPCCredentials: insecure.NewCredentials(),
					},
				},
			}

			taskID := idgen.TaskIDV1(tc.request.URL, tc.request.URLMeta)
			tc.verify(ptm.tryReuseStreamPeerTask(context.Background(), taskID, tc.request))
		})
	}
}
