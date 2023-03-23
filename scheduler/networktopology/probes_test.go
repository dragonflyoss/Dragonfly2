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
	mockDestHost = &resource.Host{
		ID:              idgen.HostIDV2("127.0.0.1", "destHostName"),
		Type:            types.HostTypeNormal,
		Hostname:        "destination_hostname",
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

	mockSrcHost = &resource.Host{
		ID:              idgen.HostIDV2("127.0.0.1", "srcHostName"),
		Type:            types.HostTypeNormal,
		Hostname:        "source_hostname",
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

	mockProbesWithOneProbe = []*Probe{
		{
			Host:      mockDestHost,
			RTT:       30 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
	}

	mockProbesWithThreeProbe = []*Probe{
		{
			Host:      mockDestHost,
			RTT:       30 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       31 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       32 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
	}

	mockProbesWithSixProbe = []*Probe{
		{
			Host:      mockDestHost,
			RTT:       30 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       31 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       32 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       33 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       34 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
		{
			Host:      mockDestHost,
			RTT:       35 * time.Millisecond,
			UpdatedAt: time.Now().Local(),
		},
	}
)

func TestProbes_NewProbes(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, probes Probes)
	}{
		{
			name: "new probes",
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(probes).Elem().Name(), "probes")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes(mockSrcHost))
		})
	}
}

func TestProbes_LoadProbe(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes []*Probe
		expect    func(t *testing.T, probes Probes)
	}{
		{
			name:      "load probe from probes which has one probe",
			rawProbes: mockProbesWithOneProbe,
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				probe, loaded := probes.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbesWithOneProbe[0].Host.ID)
				assert.Equal(probe.RTT, mockProbesWithOneProbe[0].RTT)
				assert.Equal(probe.UpdatedAt, mockProbesWithOneProbe[0].UpdatedAt)
			},
		},
		{
			name:      "load probe from probes which has three probes",
			rawProbes: mockProbesWithThreeProbe,
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				probe, loaded := probes.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbesWithThreeProbe[2].Host.ID)
				assert.Equal(probe.RTT, mockProbesWithThreeProbe[2].RTT)
				assert.Equal(probe.UpdatedAt, mockProbesWithThreeProbe[2].UpdatedAt)
			},
		},
		{
			name:      "probe does not exist",
			rawProbes: []*Probe{},
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				probe, loaded := probes.LoadProbe()
				assert.Equal(loaded, false)
				assert.Nil(probe)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			for _, p := range tc.rawProbes {
				probes.StoreProbe(p)
			}
			tc.expect(t, probes)
		})
	}
}

func TestProbes_StoreProbe(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes []*Probe
		expect    func(t *testing.T, probes Probes)
	}{
		{
			name:      "store probe",
			rawProbes: mockProbesWithOneProbe,
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				assert.Equal(probes.GetQueue().Len(), 1)
				p, loaded := probes.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probes.GetAverageRTT(), p.RTT)

				assert.Equal(p.Host.ID, mockProbesWithOneProbe[0].Host.ID)
				assert.Equal(p.RTT, mockProbesWithOneProbe[0].RTT)
				assert.Equal(p.UpdatedAt, mockProbesWithOneProbe[0].UpdatedAt)

			},
		},
		{
			name:      "store probe when probes has full probe",
			rawProbes: mockProbesWithSixProbe,
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				assert.Equal(probes.GetQueue().Len(), config.DefaultProbeQueueLength)
				p, loaded := probes.LoadProbe()

				var averageRTT = float64(probes.GetQueue().Front().Value.(*Probe).RTT)
				for e := probes.GetQueue().Front().Next(); e != nil; e = e.Next() {
					averageRTT = averageRTT*0.1 + float64(e.Value.(*Probe).RTT)*0.9
				}
				assert.Equal(probes.GetAverageRTT(), time.Duration(averageRTT))

				assert.Equal(loaded, true)
				assert.Equal(p.Host.ID, mockProbesWithSixProbe[5].Host.ID)
				assert.Equal(p.RTT, mockProbesWithSixProbe[5].RTT)
				assert.Equal(p.UpdatedAt, mockProbesWithSixProbe[5].UpdatedAt)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			for _, p := range tc.rawProbes {
				probes.StoreProbe(p)
			}
			tc.expect(t, probes)
		})
	}
}

func TestProbes_GetQueue(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes []*Probe
		expect    func(t *testing.T, queue *list.List)
	}{
		{
			name:      "get Queue from probes which has only one probe",
			rawProbes: mockProbesWithOneProbe,
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), len(mockProbesWithOneProbe))
			},
		},
		{
			name:      "get update time from probes which has three probe",
			rawProbes: mockProbesWithThreeProbe,
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), len(mockProbesWithOneProbe))
			},
		},
		{
			name:      "failed to get the update time",
			rawProbes: []*Probe{},
			expect: func(t *testing.T, queue *list.List) {
				assert := assert.New(t)
				assert.Equal(queue.Len(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			for _, p := range tc.rawProbes {
				probes.StoreProbe(p)
			}
			tc.expect(t, probes.GetQueue())
		})
	}
}

func TestProbes_GetUpdatedAt(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes []*Probe
		expect    func(t *testing.T, updatedAt time.Time)
	}{
		{
			name:      "get update time from probes which has only one probe",
			rawProbes: mockProbesWithOneProbe,
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockProbesWithOneProbe[0].UpdatedAt)
			},
		},
		{
			name:      "get update time from probes which has three probe",
			rawProbes: mockProbesWithThreeProbe,
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockProbesWithThreeProbe[2].UpdatedAt)
			},
		},
		{
			name:      "failed to get the update time",
			rawProbes: []*Probe{},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, time.Time{}.UTC())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			for _, p := range tc.rawProbes {
				probes.StoreProbe(p)
			}
			tc.expect(t, probes.GetUpdatedAt())
		})
	}
}

func TestProbes_GetAverageRTT(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes []*Probe
		expect    func(t *testing.T, averageRTT time.Duration)
	}{
		{
			name:      "get average RTT from probes which has only one probes",
			rawProbes: mockProbesWithOneProbe,
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT, mockProbesWithOneProbe[0].RTT)
			},
		},
		{
			name:      "get average RTT from probes which has three probes",
			rawProbes: mockProbesWithThreeProbe,
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)

				var a = float64(mockProbesWithThreeProbe[0].RTT)
				for _, p := range mockProbesWithThreeProbe {
					a = a*0.1 + float64(p.RTT)*0.9
				}

				assert.Equal(averageRTT, time.Duration(a))
			},
		},
		{
			name:      "failed to get the average RTT",
			rawProbes: []*Probe{},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT, time.Duration(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			for _, p := range tc.rawProbes {
				probes.StoreProbe(p)
			}
			tc.expect(t, probes.GetAverageRTT())
		})
	}
}
