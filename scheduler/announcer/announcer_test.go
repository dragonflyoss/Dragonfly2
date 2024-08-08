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
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	trainerv1 "d7y.io/api/v2/pkg/apis/trainer/v1"
	trainerv1mocks "d7y.io/api/v2/pkg/apis/trainer/v1/mocks"

	managerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	trainerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/trainer/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

var (
	mockIDC      = "foo"
	mockLocation = "bar"
)

type mockReadCloserWithReadError struct{}

func (m *mockReadCloserWithReadError) Read(p []byte) (int, error) {
	return 0, errors.New("foo")
}

func (m *mockReadCloserWithReadError) Close() error {
	return nil
}

func TestAnnouncer_New(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockTrainerClient := trainerclientmocks.NewMockV1(ctl)

	tests := []struct {
		name    string
		config  *config.Config
		options []Option
		mock    func(m *managerclientmocks.MockV2MockRecorder)
		expect  func(t *testing.T, announcer Announcer, err error)
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
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			options: []Option{},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.NoError(err)
				assert.NotNil(instance.config)
				assert.NotNil(instance.managerClient)
			},
		},
		{
			name: "new announcer with trainer client",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			options: []Option{WithTrainerClient(mockTrainerClient)},
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
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			options: []Option{},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockManagerClient.EXPECT())

			a, err := New(tc.config, mockManagerClient, mockStorage, tc.options...)
			tc.expect(t, a, err)
		})
	}
}

func TestAnnouncer_Serve(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockTrainerClient := trainerclientmocks.NewMockV1(ctl)

	tests := []struct {
		name    string
		config  *config.Config
		data    []byte
		options []Option
		sleep   func()
		mock    func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except  func(t *testing.T, a Announcer)
	}{
		{
			name: "started announcer server success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					KeepAlive: config.KeepAliveConfig{
						Interval: 50 * time.Millisecond,
					},
					SchedulerClusterID: 1,
				},
				Trainer: config.TrainerConfig{
					Interval:      2 * time.Second,
					UploadTimeout: 10 * time.Second,
				},
			},
			data:    []byte("bar"),
			options: []Option{WithTrainerClient(mockTrainerClient)},
			sleep: func() {
				time.Sleep(3 * time.Second)
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					m.KeepAlive(gomock.Eq(50*time.Millisecond), gomock.Eq(&managerv2.KeepAliveRequest{
						SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:   "localhost",
						Ip:         "127.0.0.1",
						ClusterId:  uint64(1),
					}), gomock.Any()).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
					mt.CloseAndRecv().Do(func() { wg.Wait() }).Return(nil, nil).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer) {
				go a.Serve()
			},
		},
		{
			name: "started announcer server success without trainer client",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					KeepAlive: config.KeepAliveConfig{
						Interval: 50 * time.Millisecond,
					},
					SchedulerClusterID: 1,
				},
			},
			data:    []byte("bar"),
			options: []Option{},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					m.KeepAlive(gomock.Eq(50*time.Millisecond), gomock.Eq(&managerv2.KeepAliveRequest{
						SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:   "localhost",
						Ip:         "127.0.0.1",
						ClusterId:  uint64(1),
					}), gomock.Any()).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer) {
				go a.Serve()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream := trainerv1mocks.NewMockTrainer_TrainClient(ctl)
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)

			tc.mock(stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())
			a, err := New(tc.config, mockManagerClient, mockStorage, tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			tc.except(t, a)
			tc.sleep()
			a.Stop()
		})
	}
}

func TestAnnouncer_announceToManager(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		sleep  func()
		mock   func(m *managerclientmocks.MockV2MockRecorder)
		except func(a Announcer)
	}{
		{
			name: "announce to manager success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					KeepAlive: config.KeepAliveConfig{
						Interval: 50 * time.Millisecond,
					},
					SchedulerClusterID: 1,
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					m.KeepAlive(gomock.Eq(50*time.Millisecond), gomock.Eq(&managerv2.KeepAliveRequest{
						SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:   "localhost",
						Ip:         "127.0.0.1",
						ClusterId:  uint64(1),
					}), gomock.Any()).Times(1),
				)
			},
			except: func(a Announcer) {
				a.(*announcer).announceToManager()
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

			a, err := New(tc.config, mockManagerClient, mockStorage, WithTrainerClient(mockTrainerClient))
			if err != nil {
				t.Fatal(err)
			}
			tc.except(a)
			tc.sleep()
		})
	}
}

func TestAnnouncer_announceToTrainer(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		sleep  func()
		mock   func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(a Announcer)
	}{
		{
			name: "announce to trainer failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
				Trainer: config.TrainerConfig{
					Interval:      2 * time.Second,
					UploadTimeout: 10 * time.Second,
				},
			},
			data: []byte("bar"),
			sleep: func() {
				time.Sleep(3 * time.Second)
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			except: func(a Announcer) {
				go a.(*announcer).announceToTrainer()
			},
		},
		{
			name: "announce to trainer success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
				Trainer: config.TrainerConfig{
					Interval:      2 * time.Second,
					UploadTimeout: 10 * time.Second,
				},
			},
			data: []byte("bar"),
			sleep: func() {
				time.Sleep(3 * time.Second)
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
					mt.CloseAndRecv().Do(func() { wg.Wait() }).Return(nil, nil).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)
			},
			except: func(a Announcer) {
				go a.(*announcer).announceToTrainer()
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
			tc.mock(stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())

			a, err := New(tc.config, mockManagerClient, mockStorage, WithTrainerClient(mockTrainerClient))
			if err != nil {
				t.Fatal(err)
			}
			tc.except(a)
			tc.sleep()
			a.Stop()
		})
	}
}

func TestAnnouncer_train(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		mock   func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "get stream failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "upload download failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return errors.New("foo")
						}).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "upload network topology failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return errors.New("foo")
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "close stream failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
					mt.CloseAndRecv().Return(nil, errors.New("foo")).Do(func() { wg.Wait() }).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "train success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				var wg sync.WaitGroup
				wg.Add(2)

				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					mtc.Train(gomock.Any()).Return(stream, nil).Times(1),
					mt.CloseAndRecv().Return(nil, nil).Do(func() { wg.Wait() }).Times(1),
				)

				gomock.InOrder(
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)

				gomock.InOrder(
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							wg.Done()
							return nil
						}).Times(1),
				)
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
			tc.mock(stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())

			a, err := New(tc.config, mockManagerClient, mockStorage, WithTrainerClient(mockTrainerClient))
			if err != nil {
				t.Fatal(err)
			}
			tc.except(t, a, a.(*announcer).train())
		})
	}
}

func TestAnnouncer_uploadDownloadToTrainer(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		mock   func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "open download failed",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenDownload().Return(nil, errors.New("foo")).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "read buffer failed",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenDownload().Return(&mockReadCloserWithReadError{}, nil).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send download request failed",
			data: []byte("bar"),
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							return nil
						}).Return(errors.New("foo")),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send download request success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenDownload().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							return nil
						}).Times(1),
				)
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
			tc.mock(stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())

			a, err := New(tc.config, mockManagerClient, mockStorage, WithTrainerClient(mockTrainerClient))
			if err != nil {
				t.Fatal(err)
			}
			tc.except(t, a, a.(*announcer).uploadDownloadToTrainer(stream))
		})
	}
}

func TestAnnouncer_uploadNetworkTopologyToTrainer(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		mock   func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "open networkTopology failed",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenNetworkTopology().Return(nil, errors.New("foo")).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "read buffer failed",
			data: []byte{},
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenNetworkTopology().Return(&mockReadCloserWithReadError{}, nil).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send network topology failed",
			data: []byte("bar"),
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							return nil
						}).Return(errors.New("foo")),
				)
			},
			except: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send network topology success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			mock: func(stream trainerv1.Trainer_TrainClient, data []byte, m *managerclientmocks.MockV2MockRecorder, mtc *trainerclientmocks.MockV1MockRecorder, ms *storagemocks.MockStorageMockRecorder, mt *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
					})).Times(1),
					ms.OpenNetworkTopology().Return(io.NopCloser(bytes.NewBuffer(data)), nil).Times(1),
					mt.Send(gomock.Any()).DoAndReturn(
						func(t *trainerv1.TrainRequest) error {
							return nil
						}).Times(1),
				)
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
			tc.mock(stream, tc.data, mockManagerClient.EXPECT(), mockTrainerClient.EXPECT(), mockStorage.EXPECT(), stream.EXPECT())

			a, err := New(tc.config, mockManagerClient, mockStorage, WithTrainerClient(mockTrainerClient))
			if err != nil {
				t.Fatal(err)
			}
			tc.except(t, a, a.(*announcer).uploadNetworkTopologyToTrainer(stream))
		})
	}
}
