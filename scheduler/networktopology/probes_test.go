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
	"container/list"
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
		ID:              idgen.HostIDV2("127.0.0.1", "hostname_seed"),
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
		Host:      mockHost,
		RTT:       30 * time.Nanosecond,
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
				assert.Equal(probes.limit, mockQueueLength)
				assert.EqualValues(probes.host, mockSeedHost)
				assert.Equal(probes.items.Len(), 0)
				assert.Equal(probes.averageRTT.Load().Nanoseconds(), int64(0))
				assert.NotEqual(probes.createdAt.Load(), 0)
				assert.Equal(probes.updatedAt.Load(), time.Time{})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes(mockQueueLength, mockSeedHost))
		})
	}
}

func TestProbes_Peek(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "queue has one probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, peeked := p.Peek()
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(p.Length(), 1)
			},
		},
		{
			name:   "queue has three probes",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 31*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 32*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, peeked := p.Peek()
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(p.Length(), 3)
			},
		},
		{
			name:   "queue has no probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				_, peeked := p.Peek()
				assert.False(peeked)
				assert.Equal(p.Length(), 0)
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

func TestProbes_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "enqueue probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.Length(), 1)

				probe, peeked := p.Peek()
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(p.AverageRTT(), probe.RTT)
			},
		},
		{
			name:   "enqueue six probes",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(NewProbe(mockHost, 34*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 31*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 32*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 33*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 34*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.Length(), 5)
				assert.Equal(p.AverageRTT().Nanoseconds(), int64(33))

				probe, peeked := p.Peek()
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
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

func TestProbes_Dequeue(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, p Probes)
	}{
		{
			name:   "dequeue probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.Length(), 1)

				probe, ok := p.Dequeue()
				assert.True(ok)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(p.AverageRTT(), probe.RTT)
				assert.Equal(p.Length(), 0)
			},
		},
		{
			name:   "dequeue probe from empty queue",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)

				_, ok := p.Dequeue()
				assert.False(ok)
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

func TestProbes_Items(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, probes *list.List)
	}{
		{
			name:   "queue has one probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, probes *list.List) {
				assert := assert.New(t)
				assert.Equal(probes.Len(), 1)

				probe, ok := probes.Front().Value.(*Probe)
				assert.True(ok)
				assert.EqualValues(probe, mockProbe)
			},
		},
		{
			name:   "queue has three probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 31*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, probes *list.List) {
				assert := assert.New(t)
				assert.Equal(probes.Len(), 3)

				frontProbe, ok := probes.Front().Value.(*Probe)
				assert.True(ok)
				assert.EqualValues(frontProbe, mockProbe)

				backProbe, ok := probes.Back().Value.(*Probe)
				assert.True(ok)
				assert.EqualValues(backProbe, mockProbe)
			},
		},
		{
			name:   "queue is empty",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, probes *list.List) {
				assert := assert.New(t)
				assert.Equal(probes.Len(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes.Items())
		})
	}
}

func TestProbes_Length(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, length int)
	}{
		{
			name:   "queue has one probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, length int) {
				assert := assert.New(t)
				assert.Equal(length, 1)
			},
		},
		{
			name:   "queue has three probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 31*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, length int) {
				assert := assert.New(t)
				assert.Equal(length, 3)
			},
		},
		{
			name:   "queue is empty",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, length int) {
				assert := assert.New(t)
				assert.Equal(length, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.probes)
			tc.expect(t, tc.probes.Length())
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
			name:   "enqueue one probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockProbe.CreatedAt)
			},
		},
		{
			name:   "enqueue three probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(NewProbe(mockHost, 10*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 100*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, updatedAt time.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt, mockProbe.CreatedAt)
			},
		},
		{
			name:   "queue is empty",
			probes: NewProbes(mockQueueLength, mockSeedHost),
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
			name:   "queue has one probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(mockProbe); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT, mockProbe.RTT)
			},
		},
		{
			name:   "queue has three probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(NewProbe(mockHost, 10*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 100*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT.Nanoseconds(), int64(36))
			},
		},
		{
			name:   "qeueu has six probe",
			probes: NewProbes(mockQueueLength, mockSeedHost),
			mock: func(probes Probes) {
				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 30*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}

				if err := probes.Enqueue(NewProbe(mockHost, 100*time.Nanosecond, time.Now())); err != nil {
					t.Fatal(err)
				}
			},
			expect: func(t *testing.T, averageRTT time.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT.Nanoseconds(), int64(93))
			},
		},
		{
			name:   "queue is empty",
			probes: NewProbes(mockQueueLength, mockSeedHost),
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
