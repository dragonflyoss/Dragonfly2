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
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
)

func Test_NewProbe(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, p *Probe)
	}{
		{
			name: "new probe",
			expect: func(t *testing.T, p *Probe) {
				assert := assert.New(t)
				assert.EqualValues(p, mockProbe)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbe(mockProbe.Host, mockProbe.RTT, mockProbe.CreatedAt))
		})
	}
}

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
				assert.Equal(probes.config.QueueLength, 5)
				assert.NotNil(probes.rdb)
				assert.Equal(probes.srcHostID, mockSeedHost.ID)
				assert.Equal(probes.destHostID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdb, _ := redismock.NewClientMock()
			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
		})
	}
}

func TestProbes_Peek(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, ps []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "queue has one probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(string(data))
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
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(rawProbes[4])
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(rawProbes[4])
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(rawProbes[0])
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probe, err := ps.Peek()
				assert.NoError(err)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.NoError(ps.Enqueue(mockProbe))

				probe, err = ps.Peek()
				assert.NoError(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)

			},
		},
		{
			name:   "queue has no probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetErr(errors.New("no probe"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Peek()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "enqueue probe when probes queue is empty",
			probes: []*Probe{
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				data, err := json.Marshal(ps[0])
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
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
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[0])).SetVal(1)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{rawProbes[1], rawProbes[0]})
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30100000)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))
			},
		},
		{
			name: "enqueue probe when probes queue has five probes",
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(rawProbes[0])
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.Nil(ps.Enqueue(mockProbe))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Dequeue(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "queue has one probe",
			probes: []*Probe{
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				data, err := json.Marshal(ps[0])
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				probe, err := ps.Dequeue()
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
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[4]))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[0]))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				assert.NoError(ps.Enqueue(mockProbe))

				probe, err := ps.Dequeue()
				assert.NoError(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)
			},
		},
		{
			name:   "dequeue probe from empty probes",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).RedisNil()
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Dequeue()
				assert.Error(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Length(t *testing.T) {
	tests := []struct {
		name   string
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, ps []*Probe)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name:   "queue has one probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Length()
				assert.NoError(err)
				assert.Equal(length, int64(1))
			},
		},
		{
			name: "queue has six probe",
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				var rawProbes []string
				for _, p := range ps {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					rawProbes = append(rawProbes, string(data))
				}

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(rawProbes[4]))
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), []byte(rawProbes[4])).SetVal(1)
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(rawProbes)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Length()
				assert.NoError(err)
				assert.Equal(length, int64(5))
				assert.NoError(ps.Enqueue(mockProbe))

				length, err = ps.Length()
				assert.NoError(err)
				assert.Equal(length, int64(5))
			},
		},
		{
			name:   "queue has no probe",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				length, err := ps.Length()
				assert.NoError(err)
				assert.Equal(length, int64(0))
			},
		},
		{
			name:   "get queue length error",
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, ps []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("get queue length error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.Length()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "get update time of probes",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt").SetVal(mockProbe.CreatedAt.Format(time.RFC3339Nano))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				updatedAt, err := ps.UpdatedAt()
				assert.NoError(err)
				assert.True(updatedAt.Equal(mockProbe.CreatedAt))
			},
		},
		{
			name: "get update time of probes error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt").SetErr(errors.New("get update time of probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.UpdatedAt()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, ps Probes)
	}{
		{
			name: "get averageRTT of probes",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT").SetVal(strconv.FormatInt(mockProbe.RTT.Nanoseconds(), 10))
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
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT").SetErr(errors.New("get averageRTT of probes error"))
			},
			expect: func(t *testing.T, ps Probes) {
				assert := assert.New(t)
				_, err := ps.AverageRTT()
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient)

			tc.expect(t, NewProbes(mockNetworkTopologyConfig.Probe, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}
