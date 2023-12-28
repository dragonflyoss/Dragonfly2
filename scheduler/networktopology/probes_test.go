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
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

var (
	mockHost = &resource.Host{
		ID:                    idgen.HostIDV2("127.0.0.1", "foo"),
		Type:                  types.HostTypeNormal,
		Hostname:              "foo",
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
		ID:                    idgen.HostIDV2("127.0.0.1", "bar"),
		Type:                  types.HostTypeSuperSeed,
		Hostname:              "bar",
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
			Count:       10,
		},
	}

	mockNetworkTopology = map[string]string{
		"createdAt":  time.Now().Format(time.RFC3339Nano),
		"updatedAt":  time.Now().Format(time.RFC3339Nano),
		"averageRTT": strconv.FormatInt(mockProbe.RTT.Nanoseconds(), 10),
	}

	mockCacheExpiration = time.Now()
	mockProbedCount     = 10
)

func Test_NewProbes(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, rawProbes Probes)
	}{
		{
			name: "new probes",
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probes := ps.(*probes)
				assert.Equal(probes.config.Probe.QueueLength, 5)
				assert.NotNil(probes.rdb)
				assert.Equal(probes.srcHostID, mockSeedHost.ID)
				assert.Equal(probes.destHostID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			rdb, _ := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
		})
	}
}

func TestProbes_Peek(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "queue has one probe",
			probes: []*Probe{mockProbe},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probe, err := ps.Peek()
				assert.NoError(err)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.Host.Type, mockProbe.Host.Type)
				assert.Equal(probe.Host.Hostname, mockProbe.Host.Hostname)
				assert.Equal(probe.Host.IP, mockProbe.Host.IP)
				assert.Equal(probe.Host.Port, mockProbe.Host.Port)
				assert.Equal(probe.Host.DownloadPort, mockProbe.Host.DownloadPort)
				assert.Equal(probe.Host.OS, mockProbe.Host.OS)
				assert.Equal(probe.Host.Platform, mockProbe.Host.Platform)
				assert.Equal(probe.Host.PlatformFamily, mockProbe.Host.PlatformFamily)
				assert.Equal(probe.Host.PlatformVersion, mockProbe.Host.PlatformVersion)
				assert.Equal(probe.Host.KernelVersion, mockProbe.Host.KernelVersion)
				assert.Equal(probe.Host.ConcurrentUploadCount, mockProbe.Host.ConcurrentUploadCount)
				assert.Equal(probe.Host.UploadCount, mockProbe.Host.UploadCount)
				assert.Equal(probe.Host.UploadFailedCount, mockProbe.Host.UploadFailedCount)
				assert.EqualValues(probe.Host.CPU, mockProbe.Host.CPU)
				assert.EqualValues(probe.Host.Memory, mockProbe.Host.Memory)
				assert.EqualValues(probe.Host.Network, mockProbe.Host.Network)
				assert.EqualValues(probe.Host.Disk, mockProbe.Host.Disk)
				assert.EqualValues(probe.Host.Build, mockProbe.Host.Build)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.True(probe.CreatedAt.Equal(mockProbe.CreatedAt))
			},
		},
		{
			name: "queue has six probe",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				{mockHost, 32 * time.Millisecond, time.Now()},
				{mockHost, 33 * time.Millisecond, time.Now()},
				{mockHost, 34 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(ps, mockCacheExpiration, true)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(rawProbes[4])
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(6)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(ps, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probe, err := ps.Peek()
				assert.NoError(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)
				assert.NoError(ps.Enqueue(mockProbe))

				probe, err = ps.Peek()
				assert.NoError(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)
			},
		},
		{
			name:   "queue has no probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetErr(errors.New("no probe"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Peek()
				assert.EqualError(err, "no probe")
			},
		},
		{
			name:   "type convert error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Peek()
				assert.EqualError(err, "get probes failed")
			},
		},
		{
			name:   "probes cache is empty",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return([]*Probe{}, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Peek()
				assert.EqualError(err, "probes cache is empty")
			},
		},
		{
			name:   "unmarshal probe error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{"foo"})
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Peek()
				assert.EqualError(err, "invalid character 'o' in literal false (expecting 'a')")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT(), tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "enqueue probe when probes queue is empty",
			probes: []*Probe{
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(ps[0])
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))
			},
		},
		{
			name: "enqueue probe when probes queue has one probe",
			probes: []*Probe{
				mockProbe,
				{mockHost, 31 * time.Millisecond, time.Now()},
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{rawProbes[0]})
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.All())
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[0])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{rawProbes[1], rawProbes[0]})
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30100000)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(2)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))
			},
		},
		{
			name: "enqueue probe when probes queue has five probes",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				{mockHost, 32 * time.Millisecond, time.Now()},
				{mockHost, 33 * time.Millisecond, time.Now()},
				{mockHost, 34 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(rawProbes[0])
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(6)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))
			},
		},
		{
			name:   "get the length of the queue error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetErr(errors.New("get the length of the queue error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "get the length of the queue error")
			},
		},
		{
			name: "remove the oldest probe error when the queue is full",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				{mockHost, 32 * time.Millisecond, time.Now()},
				{mockHost, 33 * time.Millisecond, time.Now()},
				{mockHost, 34 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("remove the oldest probe error when the queue is full"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "remove the oldest probe error when the queue is full")
			},
		},
		{
			name:   "marshal probe error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(&Probe{
					Host: &resource.Host{
						CPU: resource.CPU{
							Percent: math.NaN(),
						},
					},
					RTT:       30 * time.Millisecond,
					CreatedAt: time.Now(),
				}), "json: unsupported value: NaN")
			},
		},
		{
			name:   "push probe in queue error",
			probes: []*Probe{mockProbe},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(ps[0])
				if err != nil {
					t.Fatal(err)
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetErr(
					errors.New("push probe in queue error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "push probe in queue error")
			},
		},
		{
			name: "get probes error",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[1])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetErr(
					errors.New("get probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "get probes error")
			},
		},
		{
			name: "unmarshal probe error",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[1])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{"foo"})
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "invalid character 'o' in literal false (expecting 'a')")
			},
		},
		{
			name:   "update the moving average round-trip time error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
					"averageRTT", mockProbe.RTT.Nanoseconds()).SetErr(errors.New("update the moving average round-trip time error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "update the moving average round-trip time error")
			},
		},
		{
			name:   "update the updated time error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
					"averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
					"updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetErr(errors.New("update the updated time error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "update the updated time error")
			},
		},
		{
			name:   "update the number of times the host has been probed error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
					"averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
					"updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("update the number of times the host has been probed error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "update the number of times the host has been probed error")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT(), tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

// 1
func TestProbes_Len(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name:   "queue has one probe",
			probes: []*Probe{mockProbe},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Len()
				assert.NoError(err)
				assert.Equal(length, int64(1))
			},
		},
		{
			name: "queue has six probe",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				{mockHost, 32 * time.Millisecond, time.Now()},
				{mockHost, 33 * time.Millisecond, time.Now()},
				{mockHost, 34 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(ps, mockCacheExpiration, true)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[4]))
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(6)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(ps, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Len()
				assert.NoError(err)
				assert.Equal(length, int64(5))
				assert.NoError(ps.Enqueue(mockProbe))

				length, err = ps.Len()
				assert.NoError(err)
				assert.Equal(length, int64(5))
			},
		},
		{
			name:   "queue has no probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(nil)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Len()
				assert.NoError(err)
				assert.Equal(length, int64(0))
			},
		},
		{
			name:   "get queue length error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetErr(errors.New("get queue length error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Len()
				assert.EqualError(err, "get queue length error")
			},
		},
		{
			name:   "type convert error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "get probes failed")
			},
		},
		{
			name: "unmarshal probe error",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{"foo"})
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.EqualError(ps.Enqueue(mockProbe), "invalid character 'o' in literal false (expecting 'a')")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT(), tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_CreatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "get creation time of probes",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockCache.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any())
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				createdAt, err := ps.CreatedAt()
				assert.NoError(err)
				assert.Equal(createdAt.Format(time.RFC3339Nano), mockNetworkTopology["createdAt"])
			},
		},
		{
			name: "get creation time of probes error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("get creation time of probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.CreatedAt()
				assert.EqualError(err, "get creation time of probes error")
			},
		},
		{
			name: "type convert error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.CreatedAt()
				assert.EqualError(err, "get networkTopology failed")
			},
		},
		{
			name: "time parse error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(map[string]string{"createdAt": "foo"}, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.CreatedAt()
				assert.EqualError(err, "parsing time \"foo\" as \"2006-01-02T15:04:05.999999999Z07:00\": cannot parse \"foo\" as \"2006\"")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "get update time of probes",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockCache.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any())
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				updatedAt, err := ps.UpdatedAt()
				assert.NoError(err)
				assert.Equal(updatedAt.Format(time.RFC3339Nano), mockNetworkTopology["updatedAt"])
			},
		},
		{
			name: "get update time of probes error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("get update time of probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.UpdatedAt()
				assert.EqualError(err, "get update time of probes error")
			},
		},
		{
			name: "type convert error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.UpdatedAt()
				assert.EqualError(err, "get networkTopology failed")
			},
		},
		{
			name: "time parse error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(map[string]string{"updatedAt": "foo"}, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.UpdatedAt()
				assert.EqualError(err, "parsing time \"foo\" as \"2006-01-02T15:04:05.999999999Z07:00\": cannot parse \"foo\" as \"2006\"")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "get averageRTT of probes",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockCache.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any())
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				averageRTT, err := ps.AverageRTT()
				assert.NoError(err)
				assert.Equal(averageRTT, mockProbe.RTT)
			},
		},
		{
			name: "get averageRTT of probes error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("get averageRTT of probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.AverageRTT()
				assert.EqualError(err, "get averageRTT of probes error")
			},
		},
		{
			name: "get averageRTT of probes with cache",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				averageRTT, err := ps.AverageRTT()
				assert.NoError(err)
				assert.Equal(averageRTT, mockProbe.RTT)
			},
		},
		{
			name: "type convert error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.AverageRTT()
				assert.EqualError(err, "get networkTopology failed")
			},
		},
		{
			name: "parseInt error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(map[string]string{"averageRTT": "foo"}, mockCacheExpiration, true)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.AverageRTT()
				assert.EqualError(err, "strconv.ParseInt: parsing \"foo\": invalid syntax")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_dequeue(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "queue has one probe",
			probes: []*Probe{
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				data, err := json.Marshal(ps[0])
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probe, err := ps.(*probes).dequeue()
				assert.NoError(err)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.Host.Type, mockProbe.Host.Type)
				assert.Equal(probe.Host.Hostname, mockProbe.Host.Hostname)
				assert.Equal(probe.Host.IP, mockProbe.Host.IP)
				assert.Equal(probe.Host.Port, mockProbe.Host.Port)
				assert.Equal(probe.Host.DownloadPort, mockProbe.Host.DownloadPort)
				assert.Equal(probe.Host.OS, mockProbe.Host.OS)
				assert.Equal(probe.Host.Platform, mockProbe.Host.Platform)
				assert.Equal(probe.Host.PlatformFamily, mockProbe.Host.PlatformFamily)
				assert.Equal(probe.Host.PlatformVersion, mockProbe.Host.PlatformVersion)
				assert.Equal(probe.Host.KernelVersion, mockProbe.Host.KernelVersion)
				assert.Equal(probe.Host.ConcurrentUploadLimit, mockProbe.Host.ConcurrentUploadLimit)
				assert.Equal(probe.Host.ConcurrentUploadCount, mockProbe.Host.ConcurrentUploadCount)
				assert.Equal(probe.Host.UploadCount, mockProbe.Host.UploadCount)
				assert.Equal(probe.Host.UploadFailedCount, mockProbe.Host.UploadFailedCount)
				assert.EqualValues(probe.Host.CPU, mockProbe.Host.CPU)
				assert.EqualValues(probe.Host.Memory, mockProbe.Host.Memory)
				assert.EqualValues(probe.Host.Network, mockProbe.Host.Network)
				assert.EqualValues(probe.Host.Disk, mockProbe.Host.Disk)
				assert.EqualValues(probe.Host.Build, mockProbe.Host.Build)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.True(probe.CreatedAt.Equal(mockProbe.CreatedAt))
			},
		},
		{
			name: "queue has six probe",
			probes: []*Probe{
				{mockHost, 31 * time.Millisecond, time.Now()},
				{mockHost, 32 * time.Millisecond, time.Now()},
				{mockHost, 33 * time.Millisecond, time.Now()},
				{mockHost, 34 * time.Millisecond, time.Now()},
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockCache.Set(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), gomock.Any(), gomock.Any())
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[4]))
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockRDBClient.ExpectIncr(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(6)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[0]))
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))

				probe, err := ps.(*probes).dequeue()
				assert.NoError(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)
			},
		},
		{
			name: "dequeue probe from empty probes",
			probes: []*Probe{
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).RedisNil()
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.(*probes).dequeue()
				assert.EqualError(err, "redis: nil")
			},
		},
		{
			name:   "unmarshal probe error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, ps []*Probe) {
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal("foo")
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.(*probes).dequeue()
				assert.EqualError(err, "invalid character 'o' in literal false (expecting 'a')")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT(), tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig, rdb, cache, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}
