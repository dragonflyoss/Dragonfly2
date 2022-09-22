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

package config

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestDynconfigGetResolveSchedulerAddrs_ManagerSourceType(t *testing.T) {
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache scheduler addrs",
			expire: 10 * time.Second,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:   "127.0.0.1",
						Port: 3000,
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:   data.Schedulers[0].Ip,
								Port: data.Schedulers[0].Port,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name:   "get scheduler addrs",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:   "127.0.0.1",
						Port: 3000,
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:   data.Schedulers[0].Ip,
								Port: data.Schedulers[0].Port,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name:   "scheduler addrs can not reachable",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:   "127.0.0.1",
						Port: 3003,
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:   data.Schedulers[0].Ip,
								Port: data.Schedulers[0].Port,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues([]resolver.Address(nil), result)
			},
		},
		{
			name:   "data has duplicate scheduler addrs",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:   "127.0.0.1",
						Port: 3000,
					},
					{
						Ip:   "127.0.0.1",
						Port: 3000,
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:   data.Schedulers[0].Ip,
								Port: data.Schedulers[0].Port,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name:   "list schedulers error",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:   "127.0.0.1",
						Port: 3000,
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:   data.Schedulers[0].Ip,
								Port: data.Schedulers[0].Port,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues(result, []resolver.Address{{ServerName: "127.0.0.1", Addr: "127.0.0.1:3000"}})
			},
		},
		{
			name:   "scheduler addrs is empty",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler(nil),
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetResolveSchedulerAddrs()
				assert.NoError(err)
				assert.EqualValues([]resolver.Address(nil), result)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
				WithExpireTime(tc.expire),
				WithManagerClient(mockManagerClient),
			)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGet_ManagerSourceType(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get dynconfig cache data",
			expire: 10 * time.Second,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						HostName: "foo",
					},
				},
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.Get()
				assert.NoError(err)
				assert.EqualValues(result, &DynconfigData{
					Schedulers: []*managerv1.Scheduler{
						{
							HostName: data.Schedulers[0].HostName,
						},
					},
				})
			},
		},
		{
			name:   "resource is empty",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers:    []*managerv1.Scheduler(nil),
				ObjectStorage: &managerv1.ObjectStorage{},
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
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
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
				WithExpireTime(tc.expire),
				WithManagerClient(mockManagerClient),
			)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGetSchedulers_ManagerSourceType(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache schedulers",
			expire: 10 * time.Second,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler(nil),
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
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
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
				WithExpireTime(tc.expire),
				WithManagerClient(mockManagerClient),
			)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGetObjectStorage_ManagerSourceType(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		expire         time.Duration
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name:   "get cache object storage",
			expire: 10 * time.Second,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
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
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				ObjectStorage: &managerv1.ObjectStorage{
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				result, err := dynconfig.GetObjectStorage()
				assert.NoError(err)
				assert.EqualValues(result, (*managerv1.ObjectStorage)(nil))
			},
		},
		{
			name:   "object storage is empty",
			expire: 10 * time.Millisecond,
			config: &DaemonOption{
				Host: HostOption{
					Hostname: "foo",
				},
			},
			data: &DynconfigData{
				ObjectStorage: &managerv1.ObjectStorage{},
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
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
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
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
				WithExpireTime(tc.expire),
				WithManagerClient(mockManagerClient),
			)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.expect(t, dynconfig, tc.data)
			tc.cleanFileCache(t)
		})
	}
}
