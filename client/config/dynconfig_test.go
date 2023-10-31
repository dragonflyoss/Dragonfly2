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

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestDynconfig_NewDynconfig(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name           string
		config         *DaemonOption
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockV1MockRecorder)
		expect         func(t *testing.T, err error)
	}{
		{
			name: "new dynconfig",
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
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockV1MockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "new dynconfig and host option is empty",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockV1MockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(&managerv1.ObjectStorage{}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "new dynconfig and list scheduler error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockV1MockRecorder) {
				m.ListSchedulers(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "foo")
			},
		},
		{
			name: "new dynconfig and get object storage error",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockV1MockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Errorf(err, "foo")
			},
		},
		{
			name: "new dynconfig and object storage is not found",
			config: &DaemonOption{
				Scheduler: SchedulerOption{
					Manager: ManagerOption{
						RefreshInterval: 10 * time.Millisecond,
					},
				},
				Host: HostOption{},
				ObjectStorage: ObjectStorageOption{
					Enable: true,
				},
			},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockV1MockRecorder) {
				gomock.InOrder(
					m.ListSchedulers(gomock.Any(), gomock.Any()).Return(&managerv1.ListSchedulersResponse{}, nil).Times(1),
					m.GetObjectStorage(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.NotFound, "")).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV1(ctl)
			tc.mock(mockManagerClient.EXPECT())
			_, err := NewDynconfig(
				ManagerSourceType, tc.config,
				WithCacheDir(mockCacheDir),
				WithManagerClient(mockManagerClient),
			)
			tc.expect(t, err)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfig_validate(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockManagerClient := mocks.NewMockV1(ctl)
	tests := []struct {
		name      string
		dynconfig *dynconfig
		expect    func(t *testing.T, err error)
	}{
		{
			name: "validate dynconfig",
			dynconfig: &dynconfig{
				sourceType:    ManagerSourceType,
				managerClient: mockManagerClient,
				cacheDir:      "foo",
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "manager client not found",
			dynconfig: &dynconfig{
				sourceType:    ManagerSourceType,
				managerClient: nil,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager dynconfig requires parameter ManagerClient")
			},
		},
		{
			name: "cacheDir not found",
			dynconfig: &dynconfig{
				sourceType:    ManagerSourceType,
				managerClient: mockManagerClient,
				cacheDir:      "",
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager dynconfig requires parameter CacheDir")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, tc.dynconfig.validate())
		})
	}
}
