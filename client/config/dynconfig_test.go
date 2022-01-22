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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/config/mocks"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
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
				m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:       "new dynconfig without empty host option",
			expire:     10 * time.Millisecond,
			hostOption: HostOption{},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:           "new dynconfig with list scheduler error",
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
			name:   "new dynconfig without expire time",
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
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
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
			name:   "list schedulers empty",
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
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
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
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{
						Schedulers: []*manager.Scheduler{
							{
								HostName: data.Schedulers[0].HostName,
							},
						},
					}, nil).Times(1),
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
			name:   "list schedulers empty",
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
					m.ListSchedulers(gomock.Any()).Return(&manager.ListSchedulersResponse{}, nil).Times(1),
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
