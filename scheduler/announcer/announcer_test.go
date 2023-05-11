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

package announcer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"
	trainerv1mocks "d7y.io/api/pkg/apis/trainer/v1/mocks"

	managerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	trainerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/trainer/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

func TestAnnouncer_New(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(m *managerclientmocks.MockV2MockRecorder)
		expect func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "new announcer",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      "foo",
					Location: "bar",
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.NoError(err)
				assert.NotNil(instance.config)
				assert.NotNil(instance.managerClient)
				assert.NotNil(instance.trainerClient)
				assert.NotNil(instance.storage)
			},
		},
		{
			name: "update scheduler failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      "foo",
					Location: "bar",
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			mockTrainerClient := trainerclientmocks.NewMockV1(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockManagerClient.EXPECT())

			a, err := New(tc.config, mockManagerClient, WithTrainerClient(mockTrainerClient), WithStorage(mockStorage))
			tc.expect(t, a, err)
		})
	}
}

func TestAnnouncer_transferDataToTrainer(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		mock   func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "get stream error",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte{},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "open download error",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(stream, nil)
				ms.OpenDownload().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send download error",
			data: []byte("hello, world"),
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(stream, nil).Times(1)
				ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				mt.Send(gomock.Any()).Return(errors.New("foo"))
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "open networkTopology error",
			data: []byte("hello, world"),
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(stream, nil).Times(1)
				ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				mt.Send(gomock.Any()).DoAndReturn(
					func(t *trainerv1.TrainRequest) error {
						return nil
					}).AnyTimes()
				ms.OpenNetworkTopology().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send networkTopology error",
			data: []byte("hello, world"),
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							return nil
						}).AnyTimes(),
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil),
					mt.Send(gomock.Any()).Return(errors.New("foo")),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "close stream error",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(stream, nil).Times(1)
				ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				mt.Send(gomock.Any()).DoAndReturn(
					func(t *trainerv1.TrainRequest) error {
						return nil
					}).AnyTimes()
				mt.CloseAndRecv().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("hello, world"),
			mock: func(t *testing.T, stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mtc.Train(gomock.Any()).Return(stream, nil).Times(1)
				ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil)
				mt.Send(gomock.Any()).DoAndReturn(
					func(t *trainerv1.TrainRequest) error {
						return nil
					}).AnyTimes()
				mt.CloseAndRecv().Return(nil, nil)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := trainerv1mocks.NewMockTrainer_TrainClient(ctl)
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			mockTrainerClient := trainerclientmocks.NewMockV1(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)

			tc.mock(t, stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())
			a, err := New(tc.config, mockManagerClient, WithTrainerClient(mockTrainerClient), WithStorage(mockStorage))
			if err != nil {
				t.Fatal(err)
			}
			err = a.(*announcer).transferDataToTrainer(context.Background())
			tc.except(t, a, err)
		})
	}
}
