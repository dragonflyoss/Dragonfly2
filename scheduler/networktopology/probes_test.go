package networktopology

import (
	"container/list"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
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
		SecurityDomain:           mockHostSecurityDomain,
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

	mockHostSecurityDomain = "security_domain"
	mockHostLocation       = "location"
	mockHostIDC            = "idc"

	mockProbe = &Probe{
		Host:      mockDestHost,
		RTT:       mockRTT,
		UpdatedAt: mockUpdatedAt,
	}

	mockRTT       = 30 * time.Millisecond
	mockUpdatedAt = time.Now()

	mockProbes = &Probes{
		Host:       mockSrcHost,
		Probes:     mockList,
		AverageRTT: mockAverageRTT,
	}

	mockList       = list.New()
	mockAverageRTT = time.Duration(0)

	mockInitTime = time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
)

func TestProbes_NewProbe(t *testing.T) {
	tests := []struct {
		name     string
		rawProbe *Probe
		expect   func(t *testing.T, probe *Probe)
	}{
		{
			name:     "new probe",
			rawProbe: mockProbe,
			expect: func(t *testing.T, probe *Probe) {
				assert := assert.New(t)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.Host.Port, mockProbe.Host.Port)
				assert.Equal(probe.Host.DownloadPort, mockProbe.Host.DownloadPort)
				assert.Equal(probe.Host.Network.SecurityDomain, mockProbe.Host.Network.SecurityDomain)
				assert.Equal(probe.Host.Network.Location, mockProbe.Host.Network.Location)
				assert.Equal(probe.Host.Network.IDC, mockProbe.Host.Network.IDC)
				assert.NotEqual(probe.Host.UpdatedAt.Load(), mockProbe.Host.UpdatedAt.Load())
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.UpdatedAt, mockProbe.UpdatedAt)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbe(tc.rawProbe.Host,
				durationpb.New(tc.rawProbe.RTT),
				timestamppb.New(tc.rawProbe.UpdatedAt)))
		})
	}
}

func TestProbes_NewProbes(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes *Probes
		expect    func(t *testing.T, probes *Probes)
	}{
		{
			name:      "new probes",
			rawProbes: mockProbes,
			expect: func(t *testing.T, probes *Probes) {
				assert := assert.New(t)
				assert.Equal(probes.Host.ID, mockProbes.Host.ID)
				assert.Equal(probes.Host.Port, mockProbes.Host.Port)
				assert.Equal(probes.Host.DownloadPort, mockProbes.Host.DownloadPort)
				assert.Equal(probes.Host.Network.SecurityDomain, mockProbes.Host.Network.SecurityDomain)
				assert.Equal(probes.Host.Network.Location, mockProbes.Host.Network.Location)
				assert.Equal(probes.Host.Network.IDC, mockProbes.Host.Network.IDC)
				assert.NotEqual(probes.Host.UpdatedAt.Load(), mockProbes.Host.UpdatedAt.Load())
				assert.Equal(probes.Probes, mockProbes.Probes)
				assert.Equal(probes.AverageRTT, mockProbes.AverageRTT)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes(tc.rawProbes.Host))
		})
	}
}

func TestProbes_LoadProbe(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes *Probes
		expect    func(t *testing.T, probe *Probe, loaded bool)
	}{
		{
			name:      "load probe",
			rawProbes: mockProbes,
			expect: func(t *testing.T, probe *Probe, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.UpdatedAt, mockProbe.UpdatedAt)
			},
		},
		{
			name:      "probe does not exist",
			rawProbes: mockProbes,
			expect: func(t *testing.T, probe *Probe, loaded bool) {
				assert := assert.New(t)
				assert.Nil(probe)
				assert.Equal(loaded, false)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			probe, loaded := probes.LoadProbe()
			tc.expect(t, probe, loaded)
		})
	}
}

func TestProbes_StoreProbe(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes *Probes
		expect    func(t *testing.T, probe *Probe, loaded bool)
	}{
		{
			name:      "store probe",
			rawProbes: mockProbes,
			expect: func(t *testing.T, probe *Probe, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.UpdatedAt, mockProbe.UpdatedAt)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			probe, loaded := probes.LoadProbe()
			tc.expect(t, probe, loaded)
		})
	}
}

func TestProbes_GetUpdatedAt(t *testing.T) {
	tests := []struct {
		name      string
		rawProbes *Probes
		expect    func(t *testing.T, updatedAt time.Time, loaded bool)
	}{
		{
			name:      "get update time",
			rawProbes: mockProbes,
			expect: func(t *testing.T, updatedAt time.Time, loaded bool) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockProbe.UpdatedAt)
				assert.Equal(loaded, true)
			},
		},
		{
			name:      "failed to get the update time",
			rawProbes: mockProbes,
			expect: func(t *testing.T, updatedAt time.Time, loaded bool) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockInitTime)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			updatedAt, loaded := probes.GetUpdatedAt()
			tc.expect(t, updatedAt, loaded)
		})
	}
}

func TestProbes_GetAverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, averageRTT time.Duration, loaded bool)
	}{
		{
			name: "get average RTT",
			expect: func(t *testing.T, averageRTT time.Duration, loaded bool) {
				assert := assert.New(t)
				assert.Equal(averageRTT, mockProbe.UpdatedAt)
				assert.Equal(loaded, true)
			},
		},
		{
			name: "failed to get the update time",
			expect: func(t *testing.T, averageRTT time.Duration, loaded bool) {
				assert := assert.New(t)
				assert.Equal(averageRTT, time.Duration(0))
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			averageRTT, loaded := probes.GetAverageRTT()
			tc.expect(t, averageRTT, loaded)
		})
	}
}
