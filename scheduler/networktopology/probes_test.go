package networktopology

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
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
		RTT:       3000000 * time.Nanosecond,
		CreatedAt: time.Now(),
	}

	mockQueueLength = 5
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
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "new probes",
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probes := p.(*probes)
				assert.NotNil(probes.rdb)
				assert.Equal(probes.limit, mockQueueLength)
				assert.Equal(probes.src, mockSeedHost.ID)
				assert.Equal(probes.dest, mockHost.ID)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdb, _ := redismock.NewClientMock()
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
		})
	}
}

func TestProbes_Peek(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLIndex(key, 0).SetVal(string(data))
				clientMock.ExpectLLen(key).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				probe, peeked := p.Peek()
				assert.ObjectsAreEqualValues(probe, mockProbe)
				a.True(peeked)
				a.Equal(p.Length(), int64(1))
			},
		},
		{
			name: "queue has no probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLIndex(key, 0).SetErr(errors.New("no probe"))
				clientMock.ExpectLLen(key).SetVal(0)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, peeked := p.Peek()
				assert.False(peeked)
				assert.Equal(p.Length(), int64(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "enqueue one probe when probes queue is null",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				probesKey := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(probesKey).SetVal(0)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(probesKey, data).SetVal(1)

				networkTopologyKey := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.MatchExpectationsInOrder(false)
				clientMock.ExpectHSet(networkTopologyKey, "averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "createdAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "updatedAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				err := p.Enqueue(mockProbe)
				a.Nil(err)
			},
		},
		{
			name: "enqueue one probe when probes queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				probesKey := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(probesKey).SetVal(1)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(probesKey, data).SetVal(1)

				mockData, err := json.Marshal(NewProbe(mockHost, 3100000*time.Nanosecond, time.Now()))
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLRange(probesKey, 0, -1).SetVal([]string{string(mockData), string(data)})

				networkTopologyKey := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.MatchExpectationsInOrder(false)
				clientMock.ExpectHSet(networkTopologyKey, "averageRTT", int64(2979000)).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "updatedAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				err := p.Enqueue(mockProbe)
				a.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_Dequeue(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "dequeue probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				key := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLPop(key).SetVal(string(data))
				clientMock.ExpectLIndex(key, 0).RedisNil()
				clientMock.ExpectLLen(key).SetVal(0)
			},
			expect: func(t *testing.T, p Probes) {
				a := assert.New(t)
				probe, ok := p.Dequeue()
				assert.ObjectsAreEqualValues(probe, mockProbe)
				a.True(ok)

				_, peeked := p.Peek()
				a.False(peeked)
				a.Equal(p.Length(), int64(0))
			},
		},
		{
			name: "dequeue probe from empty probes",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLPop(key).RedisNil()
				clientMock.ExpectLIndex(key, 0).RedisNil()
				clientMock.ExpectLLen(key).SetVal(0)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, ok := p.Dequeue()
				assert.False(ok)

				_, peeked := p.Peek()
				assert.False(peeked)
				assert.Equal(p.Length(), int64(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_Length(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(key).SetVal(1)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.Length(), int64(1))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_CreatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "get creation time of probes",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(key, "createdAt").SetVal(strconv.FormatInt(mockProbe.CreatedAt.UnixNano(), 10))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.CreatedAt().UnixNano(), mockProbe.CreatedAt.UnixNano())
			},
		},
		{
			name: "get creation time of probes error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(key, "createdAt").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.CreatedAt(), time.Time{})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "get update time of probes",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				fmt.Println(mockHost.CreatedAt.Load().Nanosecond())
				clientMock.ExpectHGet(key, "updatedAt").SetVal(strconv.FormatInt(mockProbe.CreatedAt.UnixNano(), 10))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.UpdatedAt().UnixNano(), mockProbe.CreatedAt.UnixNano())
			},
		},
		{
			name: "get update time of probes error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(key, "updatedAt").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.UpdatedAt(), time.Time{})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}

func TestProbes_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "get averageRTT of probes",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(key, "averageRTT").SetVal(strconv.FormatInt(mockProbe.RTT.Nanoseconds(), 10))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.AverageRTT(), mockProbe.RTT)
			},
		},
		{
			name: "get averageRTT of probes error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(key, "averageRTT").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.AverageRTT(), time.Duration(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			tc.expect(t, NewProbes(rdb, mockQueueLength, mockSeedHost.ID, mockHost.ID))
			clientMock.ClearExpect()
		})
	}
}
