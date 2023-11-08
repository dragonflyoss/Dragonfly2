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

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestDynconfigManager_GetResolveSchedulerAddrs(t *testing.T) {
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			panic(err)
		}
	}()
	defer grpcServer.Stop()

	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockV1MockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name: "get cache scheduler ip addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Second,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "get scheduler ip addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "get scheduler host addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Ip:       "101.1.1.1",
						Hostname: "localhost",
						Port:     3000,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Ip:       data.Schedulers[0].Ip,
								Hostname: data.Schedulers[0].Hostname,
								Port:     data.Schedulers[0].Port,
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
				assert.EqualValues(result, []resolver.Address{{ServerName: "localhost", Addr: "localhost:3000"}})
			},
		},
		{
			name: "scheduler addrs can not reachable",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
				_, err := dynconfig.GetResolveSchedulerAddrs()
				assert.EqualError(err, "can not found available scheduler addresses")
			},
		},
		{
			name: "data has duplicate scheduler addrs",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "list schedulers error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "scheduler addrs is empty",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				_, err := dynconfig.GetResolveSchedulerAddrs()
				assert.EqualError(err, "schedulers not found")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV1(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
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

func TestDynconfigManager_Get(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockV1MockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name: "get dynconfig cache data",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Second,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "get dynconfig data",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "disable object storage",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: false,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "list schedulers error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "get object storage error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "object storage is not found",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
							},
						},
					}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
							Hostname: data.Schedulers[0].Hostname,
						},
					},
					ObjectStorage: &managerv1.ObjectStorage{
						Name: data.ObjectStorage.Name,
					},
				})
			},
		},
		{
			name: "resource is empty",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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

			mockManagerClient := mocks.NewMockV1(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
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

func TestDynconfigManager_GetSchedulers(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockV1MockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name: "get cache schedulers",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Second,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
					},
				},
			},
			sleep: func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "get schedulers",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "list schedulers error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			data: &DynconfigData{
				Schedulers: []*managerv1.Scheduler{
					{
						Hostname: "foo",
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{
						Schedulers: []*managerv1.Scheduler{
							{
								Hostname: data.Schedulers[0].Hostname,
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
			name: "schedulers is empty",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, dynconfig Dynconfig, data *DynconfigData) {
				assert := assert.New(t)
				_, err := dynconfig.GetSchedulers()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV1(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
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

func TestDynconfigManager_GetObjectStorage(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		config         *DaemonOption
		data           *DynconfigData
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockV1MockRecorder, data *DynconfigData)
		expect         func(t *testing.T, dynconfig Dynconfig, data *DynconfigData)
	}{
		{
			name: "get cache object storage",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Second,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "get object storage",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "get object storage error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
			name: "object storage is not found",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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
				assert.EqualValues(result, &managerv1.ObjectStorage{
					Name: data.ObjectStorage.Name,
				})
			},
		},
		{
			name: "object storage is empty",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{
					Hostname: "foo",
				},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
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
			mock: func(m *mocks.MockV1MockRecorder, data *DynconfigData) {
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

			mockManagerClient := mocks.NewMockV1(ctl)
			tc.mock(mockManagerClient.EXPECT(), tc.data)
			dynconfig, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
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
