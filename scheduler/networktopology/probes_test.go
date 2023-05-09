package networktopology

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

var (
	mockHost = &resource.Host{
		ID:              idgen.HostIDV2("127.0.0.1", "HostName"),
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

	mockSeedHost = &resource.Host{
		ID:              idgen.HostIDV2("127.0.0.1", "HostName_seed"),
		Type:            types.HostTypeSuperSeed,
		Hostname:        "hostname_seed",
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
		CreatedAt: time.Now().UTC(),
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
		expect func(t *testing.T, p *Probes)
	}{
		{
			name: "new probes",
			expect: func(t *testing.T, p *Probes) {
				assert := assert.New(t)
				assert.Equal(p.AverageRTT, time.Duration(0))
				assert.NotEqual(p.CreatedAt, time.Now())
				assert.Equal(p.UpdatedAt, time.Time{})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes())
		})
	}
}
