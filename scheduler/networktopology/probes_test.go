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
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
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
		config config.ProbeConfig
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "new probes",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probes := p.(*probes)
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
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
		})
	}
}

func TestProbes_Peek(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, probes []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "queue has one probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(string(data))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, err := p.Peek()
				assert.Nil(err)
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
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(string(data))
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))

				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockStrs := make([]string, 0)
				for _, p := range probes {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					mockStrs = append(mockStrs, string(data))
				}

				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(mockStrs)

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetVal(mockStrs[0])
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, err := p.Peek()
				assert.Nil(err)
				assert.Equal(probe.RTT, mockProbe.RTT)

				err = p.Enqueue(mockProbe)
				assert.Nil(err)

				probe, err = p.Peek()
				assert.Nil(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)

			},
		},
		{
			name: "queue has no probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.ExpectLIndex(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0).SetErr(errors.New("no probe"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, err := p.Peek()
				assert.Error(err)
				assert.Nil(probe)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, probes []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "enqueue probe when probes queue is empty",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				a.Nil(p.Enqueue(mockProbe))
			},
		},
		{
			name: "enqueue probe when probes queue has one probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockData, err := json.Marshal(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				if err != nil {
					t.Fatal(err)
				}
				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal([]string{string(mockData), string(data)})

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30100000)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				a.Nil(p.Enqueue(mockProbe))
			},
		},
		{
			name: "enqueue probe when probes queue has five probes",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)

				mockPopProbe, err := json.Marshal(NewProbe(mockHost, 35*time.Millisecond, time.Now()))
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(mockPopProbe))

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockStrs := make([]string, 0)
				for _, p := range probes {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					mockStrs = append(mockStrs, string(data))
				}

				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(mockStrs)

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				a.Nil(p.Enqueue(mockProbe))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			tc.mock(mockRDBClient, tc.probes)
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Dequeue(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, probes []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "queue has one probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, err := p.Dequeue()
				assert.Nil(err)
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
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))

				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockStrs := make([]string, 0)
				for _, p := range probes {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					mockStrs = append(mockStrs, string(data))
				}

				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(mockStrs)

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(mockStrs[0]))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Nil(p.Enqueue(mockProbe))

				probe, err := p.Dequeue()
				assert.Nil(err)
				assert.Equal(probe.RTT, 31*time.Millisecond)

			},
		},
		{
			name: "dequeue probe from empty probes",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).RedisNil()
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, err := p.Dequeue()
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
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_Length(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		probes []*Probe
		mock   func(mockRDBClient redismock.ClientMock, probes []*Probe)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "queue has one probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				len, err := p.Length()
				assert.Nil(err)
				assert.Equal(len, int64(1))
			},
		},
		{
			name: "queue has six probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{
				NewProbe(mockHost, 31*time.Millisecond, time.Now()),
				NewProbe(mockHost, 32*time.Millisecond, time.Now()),
				NewProbe(mockHost, 33*time.Millisecond, time.Now()),
				NewProbe(mockHost, 34*time.Millisecond, time.Now()),
				mockProbe,
			},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
				mockRDBClient.ExpectLPop(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(string(data))

				mockRDBClient.ExpectRPush(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), data).SetVal(1)

				mockStrs := make([]string, 0)
				for _, p := range probes {
					data, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}

					mockStrs = append(mockStrs, string(data))
				}

				mockRDBClient.ExpectLRange(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID), 0, -1).SetVal(mockStrs)

				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT", int64(30388900)).SetVal(1)
				mockRDBClient.ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt", mockProbe.CreatedAt.Format(time.RFC3339Nano)).SetVal(1)

				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(5)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				len, err := p.Length()
				assert.Nil(err)
				assert.Equal(len, int64(5))

				assert.Nil(p.Enqueue(mockProbe))

				len, err = p.Length()
				assert.Nil(err)
				assert.Equal(len, int64(5))
			},
		},
		{
			name: "queue has no probe",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				len, err := p.Length()
				assert.Nil(err)
				assert.Equal(len, int64(0))
			},
		},
		{
			name: "get queue length error",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			probes: []*Probe{},
			mock: func(mockRDBClient redismock.ClientMock, probes []*Probe) {
				mockRDBClient.ExpectLLen(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(errors.New("get queue length error"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, err := p.Length()
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
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "get update time of probes",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt").SetVal(mockProbe.CreatedAt.Format(time.RFC3339Nano))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				updatedAt, err := p.UpdatedAt()
				assert.Nil(err)
				assert.True(updatedAt.Equal(mockProbe.CreatedAt))
			},
		},
		{
			name: "get update time of probes error",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "updatedAt").SetErr(errors.New("get update time of probes error"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, err := p.UpdatedAt()
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
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}

func TestProbes_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		config config.ProbeConfig
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "get averageRTT of probes",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT").SetVal(strconv.FormatInt(mockProbe.RTT.Nanoseconds(), 10))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				averageRTT, err := p.AverageRTT()
				assert.Nil(err)
				assert.Equal(averageRTT, mockProbe.RTT)
			},
		},
		{
			name: "get averageRTT of probes error",
			config: config.ProbeConfig{
				QueueLength: 5,
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectHGet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "averageRTT").SetErr(errors.New("get averageRTT of probes error"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, err := p.AverageRTT()
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
			tc.expect(t, NewProbes(tc.config, rdb, mockSeedHost.ID, mockHost.ID))
			mockRDBClient.ClearExpect()
		})
	}
}
