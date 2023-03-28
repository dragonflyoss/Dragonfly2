package networktopology

import (
	"container/list"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
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
				assert.Equal(reflect.TypeOf(p).Elem().Name(), "probes")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewProbes(mockConfig, mockSeedHost))
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
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.CreatedAt, mockProbe.CreatedAt)
			},
		},
		{
			name:   "load probe from probes which has three probes",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.CreatedAt, mockProbe.CreatedAt)
			},
		},
		{
			name:   "probe does not exist",
			probes: NewProbes(mockConfig, mockSeedHost),
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
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.GetProbes().Len(), 1)
				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(p.AverageRTT(), atomic.NewDuration(probe.RTT))

				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.CreatedAt, mockProbe.CreatedAt)

			},
		},
		{
			name:   "store probe when probes has full probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 33*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 34*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 35*time.Millisecond, time.Now()))
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, p Probes) {
				assert := assert.New(t)
				assert.Equal(p.GetProbes().Len(), 5)
				front, ok := p.GetProbes().Front().Value.(*Probe)
				assert.Equal(ok, true)
				averageRTT := float64(front.RTT)
				for e := p.GetProbes().Front(); e != nil; e = e.Next() {
					probe, loaded := e.Value.(*Probe)
					assert.Equal(loaded, true)
					averageRTT = float64(averageRTT)*DefaultSlidingMeanWeight +
						float64(probe.RTT)*(1-DefaultSlidingMeanWeight)
				}

				assert.Equal(p.AverageRTT().Load(), time.Duration(averageRTT))

				probe, loaded := p.LoadProbe()
				assert.Equal(loaded, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)
				assert.Equal(probe.RTT, mockProbe.RTT)
				assert.Equal(probe.CreatedAt, mockProbe.CreatedAt)
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
		expect func(t *testing.T, probes *list.List)
	}{
		{
			name:   "get probes list from probes which has only one probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, probes *list.List) {
				assert := assert.New(t)
				assert.Equal(probes.Len(), 1)
				probe, ok := probes.Front().Value.(*Probe)
				assert.Equal(ok, true)
				assert.Equal(probe.Host.ID, mockProbe.Host.ID)

			},
		},
		{
			name:   "get probes list from probes which has three probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, probes *list.List) {
				assert := assert.New(t)
				assert.Equal(probes.Len(), 3)

				frontProbe, ok := probes.Front().Value.(*Probe)
				assert.Equal(ok, true)
				assert.Equal(frontProbe.Host.ID, mockHost.ID)
				assert.Equal(frontProbe.RTT, 31*time.Millisecond)

				backProbe, ok := probes.Back().Value.(*Probe)
				assert.Equal(ok, true)
				assert.Equal(backProbe.Host.ID, mockProbe.Host.ID)
				assert.Equal(backProbe.RTT, mockProbe.RTT)
			},
		},
		{
			name:   "get probes list from probes which has no probe",
			probes: NewProbes(mockConfig, mockSeedHost),
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
			tc.expect(t, tc.probes.GetProbes())
		})
	}
}

func TestProbes_UpdatedAt(t *testing.T) {
	tests := []struct {
		name   string
		probes Probes
		mock   func(probes Probes)
		expect func(t *testing.T, updatedAt *atomic.Time)
	}{
		{
			name:   "get update time from probes which has only one probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, updatedAt *atomic.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt.Load(), mockProbe.CreatedAt)
			},
		},
		{
			name:   "get update time from probes which has three probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, updatedAt *atomic.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt.Load(), mockProbe.CreatedAt)
			},
		},
		{
			name:   "get update time from probes which has no probe",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, updatedAt *atomic.Time) {
				assert := assert.New(t)
				assert.Equal(updatedAt.Load(), time.Time{})
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
		expect func(t *testing.T, averageRTT *atomic.Duration)
	}{
		{
			name:   "get average rtt from probes which has only one probes",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, averageRTT *atomic.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT.Load(), mockProbe.RTT)
			},
		},
		{
			name:   "get average rtt from probes which has three probes",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock: func(probes Probes) {
				probes.StoreProbe(NewProbe(mockHost, 31*time.Millisecond, time.Now()))
				probes.StoreProbe(NewProbe(mockHost, 32*time.Millisecond, time.Now()))
				probes.StoreProbe(mockProbe)
			},
			expect: func(t *testing.T, averageRTT *atomic.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT.Load(), time.Duration(3.019e7))
			},
		},
		{
			name:   "get average rtt from probes which has no probes",
			probes: NewProbes(mockConfig, mockSeedHost),
			mock:   func(probes Probes) {},
			expect: func(t *testing.T, averageRTT *atomic.Duration) {
				assert := assert.New(t)
				assert.Equal(averageRTT.Load(), time.Duration(0))
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
