package networktopology

import (
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

	mockRawProbe = &probe{
		Host:      mockHost,
		RTT:       30 * time.Millisecond,
		UpdatedAt: time.Now().Local(),
	}
)

func TestProbe_NewProbe(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, probe *probe)
	}{
		{
			name: "new probe",
			expect: func(t *testing.T, probe *probe) {
				assert := assert.New(t)
				assert.Equal(probe.Host.ID, mockRawProbe.Host.ID)
				assert.Equal(probe.Host.Port, mockRawProbe.Host.Port)
				assert.Equal(probe.Host.DownloadPort, mockRawProbe.Host.DownloadPort)
				assert.Equal(probe.Host.Network.SecurityDomain, mockRawProbe.Host.Network.SecurityDomain)
				assert.Equal(probe.Host.Network.Location, mockRawProbe.Host.Network.Location)
				assert.Equal(probe.Host.Network.IDC, mockRawProbe.Host.Network.IDC)
				assert.Equal(probe.RTT, mockRawProbe.RTT)
				assert.Equal(probe.UpdatedAt, mockRawProbe.UpdatedAt)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbe(mockRawProbe.Host,
				durationpb.New(mockRawProbe.RTT),
				timestamppb.New(mockRawProbe.UpdatedAt)))
		})
	}
}
