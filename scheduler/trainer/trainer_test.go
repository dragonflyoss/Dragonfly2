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

package trainer

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	managerv2 "d7y.io/api/pkg/apis/manager/v2"
	trainerv1 "d7y.io/api/pkg/apis/trainer/v1"
	trainerv1mocks "d7y.io/api/pkg/apis/trainer/v1/mocks"

	trainerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/trainer/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

var (
	mockTrainerConfig = config.TrainerConfig{
		Enable:   true,
		Addr:     "127.0.0.1:9000",
		Interval: 10 * time.Minute,
	}

	mockTask = storage.Task{
		ID:                    "1",
		URL:                   "example.com",
		Type:                  "normal",
		ContentLength:         2048,
		TotalPieceCount:       1,
		BackToSourceLimit:     10,
		BackToSourcePeerCount: 2,
		State:                 "Succeeded",
		CreatedAt:             time.Now().UnixNano(),
		UpdatedAt:             time.Now().UnixNano(),
	}

	mockHost = storage.Host{
		ID:                    "2",
		Type:                  "normal",
		Hostname:              "localhost",
		IP:                    "127.0.0.1",
		Port:                  8080,
		DownloadPort:          8081,
		OS:                    "linux",
		Platform:              "ubuntu",
		PlatformFamily:        "debian",
		PlatformVersion:       "1.0.0",
		KernelVersion:         "1.0.0",
		ConcurrentUploadLimit: 100,
		ConcurrentUploadCount: 40,
		UploadCount:           20,
		UploadFailedCount:     3,
		CPU: resource.CPU{
			LogicalCount:   24,
			PhysicalCount:  12,
			Percent:        0.8,
			ProcessPercent: 0.4,
			Times: resource.CPUTimes{
				User:      100,
				System:    101,
				Idle:      102,
				Nice:      103,
				Iowait:    104,
				Irq:       105,
				Softirq:   106,
				Steal:     107,
				Guest:     108,
				GuestNice: 109,
			},
		},
		Memory: resource.Memory{
			Total:              20,
			Available:          19,
			Used:               16,
			UsedPercent:        0.7,
			ProcessUsedPercent: 0.2,
			Free:               15,
		},
		Network: resource.Network{
			TCPConnectionCount:       400,
			UploadTCPConnectionCount: 200,
			Location:                 "china",
			IDC:                      "e1",
		},
		Disk: resource.Disk{
			Total:             100,
			Free:              88,
			Used:              56,
			UsedPercent:       0.9,
			InodesTotal:       200,
			InodesUsed:        180,
			InodesFree:        160,
			InodesUsedPercent: 0.6,
		},
		Build: resource.Build{
			GitVersion: "3.0.0",
			GitCommit:  "2bf4d5e",
			GoVersion:  "1.19",
			Platform:   "linux",
		},
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}

	mockParent = storage.Parent{
		ID:               "4",
		Tag:              "m",
		Application:      "db",
		State:            "Succeeded",
		Cost:             1000,
		UploadPieceCount: 10,
		Host:             mockHost,
		CreatedAt:        time.Now().UnixNano(),
		UpdatedAt:        time.Now().UnixNano(),
	}

	mockSeedHost = storage.Host{
		ID:                    "3",
		Type:                  "super",
		Hostname:              "seed_host",
		IP:                    "127.0.0.1",
		Port:                  8080,
		DownloadPort:          8081,
		OS:                    "linux",
		Platform:              "ubuntu",
		PlatformFamily:        "debian",
		PlatformVersion:       "1.0.0",
		KernelVersion:         "1.0.0",
		ConcurrentUploadLimit: 100,
		ConcurrentUploadCount: 40,
		UploadCount:           20,
		UploadFailedCount:     3,
		CPU: resource.CPU{
			LogicalCount:   24,
			PhysicalCount:  12,
			Percent:        0.8,
			ProcessPercent: 0.4,
			Times: resource.CPUTimes{
				User:      100,
				System:    101,
				Idle:      102,
				Nice:      103,
				Iowait:    104,
				Irq:       105,
				Softirq:   106,
				Steal:     107,
				Guest:     108,
				GuestNice: 109,
			},
		},
		Memory: resource.Memory{
			Total:              20,
			Available:          19,
			Used:               16,
			UsedPercent:        0.7,
			ProcessUsedPercent: 0.2,
			Free:               15,
		},
		Network: resource.Network{
			TCPConnectionCount:       400,
			UploadTCPConnectionCount: 200,
			Location:                 "china",
			IDC:                      "e1",
		},
		Disk: resource.Disk{
			Total:             100,
			Free:              88,
			Used:              56,
			UsedPercent:       0.9,
			InodesTotal:       200,
			InodesUsed:        180,
			InodesFree:        160,
			InodesUsedPercent: 0.6,
		},
		Build: resource.Build{
			GitVersion: "3.0.0",
			GitCommit:  "2bf4d5e",
			GoVersion:  "1.19",
			Platform:   "linux",
		},
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}

	mockParents = append(make([]storage.Parent, 19), mockParent)

	mockDownload = storage.Download{
		ID:          "5",
		Tag:         "d",
		Application: "mq",
		State:       "Succeeded",
		Error: storage.Error{
			Code:    "unknow",
			Message: "unknow",
		},
		Cost:      1000,
		Task:      mockTask,
		Host:      mockHost,
		Parents:   mockParents,
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}

	mockNetworkTopology = storage.NetworkTopology{
		ID:        "6",
		Host:      mockSeedHost,
		DestHosts: mockDestHosts,
	}

	mockDestHost = storage.DestHost{
		Host: mockHost,
		Probes: storage.Probes{
			AverageRTT: 10,
			CreatedAt:  time.Now().UnixNano(),
			UpdatedAt:  time.Now().UnixNano(),
		},
	}

	mockDestHosts = append(make([]storage.DestHost, 9), mockDestHost)
)

func Test_New(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *trainerclientmocks.MockV1MockRecorder, stream trainerv1.Trainer_TrainClient)
		expect func(t *testing.T, tr Trainer, err error)
	}{
		{
			name: "get trainerStream error",
			mock: func(m *trainerclientmocks.MockV1MockRecorder, stream trainerv1.Trainer_TrainClient) {
				m.Train(gomock.Any()).Return(nil, errors.New("foo"))
			},
			expect: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "new trainer",
			mock: func(m *trainerclientmocks.MockV1MockRecorder, stream trainerv1.Trainer_TrainClient) {
				m.Train(gomock.Any()).Return(stream, nil)
			},
			expect: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockDynconfig := configmocks.NewMockDynconfigInterface(ctl)
			mockTrainerClient := trainerclientmocks.NewMockV1(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			stream := trainerv1mocks.NewMockTrainer_TrainClient(ctl)
			tc.mock(mockTrainerClient.EXPECT(), stream)
			tr, err := New(&mockTrainerConfig, mockDynconfig, mockTrainerClient, mockStorage)

			tc.expect(t, tr, err)
		})
	}
}

func TestTrainer_sendDataToTrainer(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder)
		except func(t *testing.T, tr Trainer, err error)
	}{
		{
			name: "get scheduler error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "list download error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send download error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return([]storage.Download{mockDownload}, nil)
				mts.Send(gomock.Any()).Return(errors.New("foo")).AnyTimes()
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "list networkTopology error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return([]storage.Download{mockDownload}, nil)
				mts.Send(gomock.Any()).Return(nil)
				ms.ListNetworkTopology().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send networkTopology error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return([]storage.Download{mockDownload}, nil)
				mts.Send(gomock.Any()).Return(nil)
				ms.ListNetworkTopology().Return([]storage.NetworkTopology{mockNetworkTopology}, nil)
				mts.Send(gomock.Any()).Return(errors.New("foo"))
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "close stream error",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return([]storage.Download{mockDownload}, nil)
				ms.ListNetworkTopology().Return([]storage.NetworkTopology{mockNetworkTopology}, nil)
				mts.Send(gomock.Any()).DoAndReturn(
					func(t *trainerv1.TrainRequest) error {
						return nil
					}).AnyTimes()
				mts.CloseAndRecv().Return(nil, errors.New("foo"))
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "send success",
			mock: func(stream trainerv1.Trainer_TrainClient, mt *trainerclientmocks.MockV1MockRecorder, md *configmocks.MockDynconfigInterfaceMockRecorder, ms *storagemocks.MockStorageMockRecorder, mts *trainerv1mocks.MockTrainer_TrainClientMockRecorder) {
				mt.Train(gomock.Any()).Return(stream, nil)
				md.GetScheduler().Return(&managerv2.Scheduler{
					Hostname: "foo",
					Ip:       "127.0.0.1",
					SchedulerCluster: &managerv2.SchedulerCluster{
						Id: 1,
					},
				}, nil)
				ms.ListDownload().Return([]storage.Download{mockDownload}, nil)
				ms.ListNetworkTopology().Return([]storage.NetworkTopology{mockNetworkTopology}, nil)
				mts.Send(gomock.Any()).DoAndReturn(
					func(t *trainerv1.TrainRequest) error {
						return nil
					}).AnyTimes()
				mts.CloseAndRecv().Return(nil, nil)
			},
			except: func(t *testing.T, tr Trainer, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			trainerClient := trainerclientmocks.NewMockV1(ctl)
			stream := trainerv1mocks.NewMockTrainer_TrainClient(ctl)

			tc.mock(stream, trainerClient.EXPECT(), dynconfig.EXPECT(), storage.EXPECT(), stream.EXPECT())
			svc, _ := New(&mockTrainerConfig, dynconfig, trainerClient, storage)
			err := svc.(*trainer).sendDataToTrainer()
			tc.except(t, svc, err)
		})
	}
}
