/*
 *     Copyright 2023 The Dragonfly Authors
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

package networktopology

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

var (
	mockHost = &resource.Host{
		ID:                    idgen.HostIDV2("127.0.0.1", "HostName"),
		Type:                  types.HostTypeNormal,
		Hostname:              "hostname",
		IP:                    "127.0.0.1",
		Port:                  8003,
		DownloadPort:          8001,
		OS:                    "darwin",
		Platform:              "darwin",
		PlatformFamily:        "Standalone Workstation",
		PlatformVersion:       "11.1",
		KernelVersion:         "20.2.0",
		ConcurrentUploadLimit: atomic.NewInt32(int32(300)),
		ConcurrentUploadCount: atomic.NewInt32(0),
		UploadCount:           atomic.NewInt64(0),
		UploadFailedCount:     atomic.NewInt64(0),
		CPU:                   mockCPU,
		Memory:                mockMemory,
		Network:               mockNetwork,
		Disk:                  mockDisk,
		Build:                 mockBuild,
		CreatedAt:             atomic.NewTime(time.Now()),
		UpdatedAt:             atomic.NewTime(time.Now()),
	}

	mockSeedHost = &resource.Host{
		ID:                    idgen.HostIDV2("127.0.0.1", "HostName_seed"),
		Type:                  types.HostTypeSuperSeed,
		Hostname:              "hostname_seed",
		IP:                    "127.0.0.1",
		Port:                  8003,
		DownloadPort:          8001,
		OS:                    "darwin",
		Platform:              "darwin",
		PlatformFamily:        "Standalone Workstation",
		PlatformVersion:       "11.1",
		KernelVersion:         "20.2.0",
		ConcurrentUploadLimit: atomic.NewInt32(int32(300)),
		ConcurrentUploadCount: atomic.NewInt32(0),
		UploadCount:           atomic.NewInt64(0),
		UploadFailedCount:     atomic.NewInt64(0),
		CPU:                   mockCPU,
		Memory:                mockMemory,
		Network:               mockNetwork,
		Disk:                  mockDisk,
		Build:                 mockBuild,
		CreatedAt:             atomic.NewTime(time.Now()),
		UpdatedAt:             atomic.NewTime(time.Now()),
	}

	mockCPU = resource.CPU{
		LogicalCount:   4,
		PhysicalCount:  2,
		Percent:        1,
		ProcessPercent: 0.5,
		Times: resource.CPUTimes{
			User:      240662.2,
			System:    317950.1,
			Idle:      3393691.3,
			Nice:      0,
			Iowait:    0,
			Irq:       0,
			Softirq:   0,
			Steal:     0,
			Guest:     0,
			GuestNice: 0,
		},
	}

	mockMemory = resource.Memory{
		Total:              17179869184,
		Available:          5962813440,
		Used:               11217055744,
		UsedPercent:        65.291858,
		ProcessUsedPercent: 41.525125,
		Free:               2749598908,
	}

	mockNetwork = resource.Network{
		TCPConnectionCount:       10,
		UploadTCPConnectionCount: 1,
		Location:                 mockHostLocation,
		IDC:                      mockHostIDC,
	}

	mockDisk = resource.Disk{
		Total:             499963174912,
		Free:              37226479616,
		Used:              423809622016,
		UsedPercent:       91.92547406065952,
		InodesTotal:       4882452880,
		InodesUsed:        7835772,
		InodesFree:        4874617108,
		InodesUsedPercent: 0.1604884305611568,
	}

	mockBuild = resource.Build{
		GitVersion: "v1.0.0",
		GitCommit:  "221176b117c6d59366d68f2b34d38be50c935883",
		GoVersion:  "1.18",
		Platform:   "darwin",
	}

	mockHostLocation = "location"
	mockHostIDC      = "idc"

	mockProbe = &Probe{
		Host:      mockHost,
		RTT:       30 * time.Millisecond,
		CreatedAt: time.Now(),
	}

	mockNetworkTopologyConfig = config.NetworkTopologyConfig{
		Enable:          true,
		CollectInterval: 2 * time.Hour,
		Probe: config.ProbeConfig{
			QueueLength: 5,
			Interval:    15 * time.Minute,
			Count:       10,
		},
	}
)

func Test_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "new network topology",
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(networkTopology).Elem().Name(), "networkTopology")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, _ := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
		})
	}
}

func TestNetworkTopology_LoadDestHostIDs(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "load one destination host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectKeys(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(
					[]string{fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)},
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				destHostIDs, err := networkTopology.LoadDestHostIDs(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(destHostIDs[0], fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID))
				assert.Equal(len(destHostIDs), 1)
			},
		},
		{
			name: "load destination hosts error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectKeys(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("destination hosts do not exist"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				_, err = networkTopology.LoadDestHostIDs(mockSeedHost.ID)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)

			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_DeleteHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetVal(1)
				mockRDBClient.ExpectDecrBy(pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID), 1).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete network topology error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("delete network topology error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Error(networkTopology.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete probes which sent by host error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("delete probes which sent by host error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Error(networkTopology.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete probes which sent to host error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetErr(errors.New("delete probes which sent to host error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Error(networkTopology.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "decrease probed count error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetVal(1)
				mockRDBClient.ExpectDecrBy(pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID), 1).SetErr(errors.New("decrease probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Error(networkTopology.DeleteHost(mockSeedHost.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)

			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_ProbedCount(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "get probed count of host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal("1")
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, err := networkTopology.ProbedCount(mockHost.ID)
				assert.NoError(err)
				assert.Equal(probedCount, uint64(1))
			},
		},
		{
			name: "get probed count of host error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("host do not exist"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				_, err = networkTopology.ProbedCount(mockHost.ID)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)

			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_LoadProbes(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "load probes",
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(ps).Elem().Name(), "probes")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, _ := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID))
		})
	}
}
