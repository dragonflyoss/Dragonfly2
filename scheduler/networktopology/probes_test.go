package networktopology

import (
	"container/list"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

var (
	mockSrcHost = &resource.Host{
		ID:              idgen.HostIDV2("127.0.0.1", "SrcHostName"),
		Type:            types.HostTypeNormal,
		Hostname:        "hostname",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockCPU,
		Memory:          mockMemory,
		Network:         mockNetwork,
		Disk:            mockDisk,
		Build:           mockBuild,
		CreatedAt:       atomic.NewTime(time.Now()),
		UpdatedAt:       atomic.NewTime(time.Now()),
	}

	mockConfig = &config.Config{
		NetworkTopology: config.NetworkTopologyConfig{
			Enable:          true,
			SyncInterval:    30 * time.Second,
			CollectInterval: 60 * time.Second,
			Probe: config.ProbeConfig{
				QueueLength:  5,
				SyncInterval: 30 * time.Second,
				SyncCount:    50,
			},
		},
	}
)

func Test_NewProbes(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, p Probes)
	}{
		{
			name: "new probes",
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(p).Elem().Name(), "Probes")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes(mockConfig, mockSrcHost))
		})
	}
}

func TestProbes_LoadProbe(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "load probe from probes which has one probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockRawProbe.Host.ID)
				assert.Equal(probe.RTT, mockRawProbe.RTT)
				assert.Equal(probe.CreatedAt, mockRawProbe.CreatedAt)
			},
		},
		{
			name:   "load probe from probes which has three probes",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockRawProbe.Host.ID)
				assert.Equal(probe.RTT, mockRawProbe.RTT)
				assert.Equal(probe.CreatedAt, mockRawProbe.CreatedAt)
			},
		},
		{
			name:   "probe does not exist",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, false)
				assert.Nil(probe)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes)
		})
	}
}

func TestProbes_StoreProbe(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "store probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.GetProbes().Len(), 1)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(p.AverageRTT(), probe.RTT)

				assert.Equal(probe.Host.ID, mockRawProbe.Host.ID)
				assert.Equal(probe.RTT, mockRawProbe.RTT)
				assert.Equal(probe.CreatedAt, mockRawProbe.CreatedAt)

			},
		},
		{
			name:   "store probe when probes has full probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 33*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 34*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 35*time.Millisecond, time.Now()))
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.GetProbes().Len(), 5)
				front, ok := p.GetProbes().Front().Value.(*Probe)
				assert.Equal(ok, true)
				averageRTT := float64(front.RTT)
				for e := p.GetProbes().Front().Next(); e != nil; e = e.Next() {
					rawProbe, loaded := e.Value.(*Probe)
					assert.Equal(loaded, true)
					averageRTT = float64(averageRTT)*DefaultMovingAverageValue +
						float64(rawProbe.RTT)*(1-DefaultMovingAverageValue)
				}

				assert.Equal(p.AverageRTT(), time.Duration(averageRTT))

				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockRawProbe.Host.ID)
				assert.Equal(probe.RTT, mockRawProbe.RTT)
				assert.Equal(probe.CreatedAt, mockRawProbe.CreatedAt)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes)
		})
	}
}

func TestProbes_GetProbes(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, queue *list.List)
	}{
		{
			name:   "get Queue from probes which has only one probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), 1)
			},
		},
		{
			name:   "get update time from probes which has three probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), 3)
			},
		},
		{
			name:   "get Queue from probes which has no probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes.GetProbes())
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, updatedAt time.Time)
	}{
		{
			name:   "get update time from probes which has only one probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockRawProbe.CreatedAt)
			},
		},
		{
			name:   "get update time from probes which has three probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockRawProbe.CreatedAt)
			},
		},
		{
			name:   "get update time from probes which has no probe",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, time.Time{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes.UpdatedAt())
		})
	}
}

func TestProbes_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, averageRTT time.Duration)
	}{
		{
			name:   "get average rtt from probes which has only one probes",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT, mockRawProbe.RTT)
			},
		},
		{
			name:   "get average rtt from probes which has three probes",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockRawProbe)
			},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				aver := float64(31 * time.Millisecond)
				aver = aver*DefaultMovingAverageValue +
					float64(32*time.Millisecond)*(1-DefaultMovingAverageValue)
				aver = aver*DefaultMovingAverageValue +
					float64(mockRawProbe.RTT)*(1-DefaultMovingAverageValue)

				assert.Equal(averageRTT, time.Duration(aver))
			},
		},
		{
			name:   "get average rtt from probes which has no probes",
			probes: NewProbes(mockConfig, mockSrcHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT, time.Duration(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes.AverageRTT())
		})
	}
}
