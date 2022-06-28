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

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestDynconfigNewDynconfig(t *testing.T) {
	mockCacheDir := t.TempDir()

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		hostOption     HostOption
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder)
		expect         func(t *testing.T, err error)
	}{
		{
			name:   "new dynconfig",
			expire: 10 * time.Second,
			hostOption: HostOption{
				Hostname: "foo",
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:       "new dynconfig and host option is empty",
			expire:     10 * time.Millisecond,
			hostOption: HostOption{},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:           "new dynconfig and list scheduler error",
			expire:         10 * time.Millisecond,
			hostOption:     HostOption{},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.ListSchedulers(gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "foo")
			},
		},
		{
			name:           "new dynconfig and get object storage error",
			expire:         10 * time.Millisecond,
			hostOption:     HostOption{},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "foo")
			},
		},
		{
			name:       "new dynconfig and object storage is not found",
			expire:     10 * time.Millisecond,
			hostOption: HostOption{},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:           "new dynconfig and list buckets error",
			expire:         10 * time.Millisecond,
			hostOption:     HostOption{},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "foo")
			},
		},
		{
			name:   "new dynconfig and expire time is empty",
			expire: 0,
			hostOption: HostOption{
				Hostname: "foo",
			},
			cleanFileCache: func(t *testing.T) {},
			mock:           func(m *mocks.MockClientMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "missing parameter Expire, use method WithExpireTime to assign")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT())
			_, err := NewDynconfig(mockManagerClient, mockCacheDir, tc.hostOption, tc.expire)
			tc.expect(t, err)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGet(t *testing.T) {
	mockCacheDir := t.TempDir()

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		hostOption     HostOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get dynconfig cache data",
			expire: 10 * time.Second,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.ObjectStorage.Name,
							},
						},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
		{
			name:   "get dynconfig data",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.ObjectStorage.Name,
							},
						},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
		{
			name:   "list schedulers error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
		{
			name:   "get object storage error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
		{
			name:   "object storage is not found",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, &DynconfigData{
					Schedulers: []*manager.Scheduler{
						{
							HostName: data.Schedulers[0].HostName,
						},
					},
				})
			},
		},
		{
			name:   "list buckets error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
		{
			name:   "resource is empty",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers:    []*manager.Scheduler(nil),
				ObjectStorage: &manager.ObjectStorage{},
				Buckets:       []*manager.Bucket(nil),
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, data)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(mockManagerClient, mockCacheDir, tc.hostOption, tc.expire)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGetSchedulers(t *testing.T) {
	mockCacheDir := t.TempDir()

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		hostOption     HostOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache schedulers",
			expire: 10 * time.Second,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
			},
			sleep: func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetSchedulers()
				assert.NoError(err)
				assert.EqualValues(result, data.Schedulers)
			},
		},
		{
			name:   "get schedulers",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetSchedulers()
				assert.NoError(err)
				assert.EqualValues(result, data.Schedulers)
			},
		},
		{
			name:   "list schedulers error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler{
					{
						HostName: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetSchedulers()
				assert.NoError(err)
				assert.EqualValues(result, data.Schedulers)
			},
		},
		{
			name:   "schedulers is empty",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Schedulers: []*manager.Scheduler(nil),
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetSchedulers()
				assert.NoError(err)
				assert.EqualValues(result, data.Schedulers)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(mockManagerClient, mockCacheDir, tc.hostOption, tc.expire)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGetObjectStorage(t *testing.T) {
	mockCacheDir := t.TempDir()

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		hostOption     HostOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache object storage",
			expire: 10 * time.Second,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
			},
			sleep: func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, data.ObjectStorage)
			},
		},
		{
			name:   "get object storage",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, data.ObjectStorage)
			},
		},
		{
			name:   "get object storage error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, data.ObjectStorage)
			},
		},
		{
			name:   "object storage is not found",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				ObjectStorage: &manager.ObjectStorage{
					Name: "foo",
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, (*manager.ObjectStorage)(nil))
			},
		},
		{
			name:   "object storage is empty",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				ObjectStorage: &manager.ObjectStorage{},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, data.ObjectStorage)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(mockManagerClient, mockCacheDir, tc.hostOption, tc.expire)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGetBuckets(t *testing.T) {
	mockCacheDir := t.TempDir()

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		hostOption     HostOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache buckets",
			expire: 10 * time.Second,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetBuckets()
				assert.NoError(err)
				assert.EqualValues(result, data.Buckets)
			},
		},
		{
			name:   "get buckets",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetBuckets()
				assert.NoError(err)
				assert.EqualValues(result, data.Buckets)
			},
		},
		{
			name:   "list buckets error",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Buckets: []*manager.Bucket{
					{
						Name: "foo",
					},
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{
						Buckets: []*manager.Bucket{
							{
								Name: data.Buckets[0].Name,
							},
						},
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetBuckets()
				assert.NoError(err)
				assert.EqualValues(result, data.Buckets)
			},
		},
		{
			name:   "buckets is empty",
			expire: 10 * time.Millisecond,
			hostOption: HostOption{
				Hostname: "foo",
			},
			data: &DynconfigData{
				Buckets: []*manager.Bucket(nil),
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any()).Return(&manager.ObjectStorage{}, nil).Times(1),
					m.ListBuckets(gomock.Any()).Return(&manager.ListBucketsResponse{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetBuckets()
				assert.NoError(err)
				assert.EqualValues(result, data.Buckets)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(mockManagerClient, mockCacheDir, tc.hostOption, tc.expire)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}
