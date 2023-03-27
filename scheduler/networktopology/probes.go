package networktopology

import (
	"container/list"
	"go.uber.org/atomic"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

// DefaultSlidingMeanParameter adjusts the impact of the previous values.
const DefaultSlidingMeanParameter = 0.1

type Probes interface {
	// LoadProbe return the latest probe.
	LoadProbe() (*Probe, bool)

	// StoreProbe stores probe in probe list.
	StoreProbe(*Probe) bool

	// GetProbes gets the probes list from struct probes
	GetProbes() *list.List

	// UpdatedAt gets the probe update time.
	UpdatedAt() *atomic.Time

	// AverageRTT gets the average RTT of probes.
	AverageRTT() *atomic.Duration
}

type probes struct {
	// config is scheduler configuration.
	config *config.Config

	// host metadata.
	host *resource.Host

	// items is the list of probe.
	items *list.List

	// averageRTT is the average round-trip time of probes.
	averageRTT *atomic.Duration

	// createdAt is the creation time of probes.
	createdAt *atomic.Time

	// updatedAt is the update time to store probe.
	updatedAt *atomic.Time
}

// NewProbes creates a new probe list instance.
func NewProbes(cfg *config.Config, host *resource.Host) Probes {
	return &probes{
		config:     cfg,
		host:       host,
		items:      list.New(),
		averageRTT: atomic.NewDuration(0),
		createdAt:  atomic.NewTime(time.Now()),
		updatedAt:  atomic.NewTime(time.Time{}),
	}
}

func (p *probes) LoadProbe() (*Probe, bool) {
	if p.items.Len() == 0 {
		return nil, false
	}

	probe, ok := p.items.Back().Value.(*Probe)
	if !ok {
		return nil, false
	}

	return probe, true
}

// StoreProbe stores probe in probe list.
func (p *probes) StoreProbe(probe *Probe) bool {
	if p.items.Len() == p.config.NetworkTopology.Probe.QueueLength {
		front := p.items.Front()
		p.items.Remove(front)
	}
	p.items.PushBack(probe)

	//update updatedAt
	p.updatedAt = atomic.NewTime(probe.CreatedAt)

	//update AverageRtt by moving average method
	front, ok := p.items.Front().Value.(*Probe)
	if ok {
		averageRTT := float64(front.RTT)
		for e := p.items.Front().Next(); e != nil; e = e.Next() {
			rawProbe, loaded := e.Value.(*Probe)
			if !loaded {
				return loaded
			}

			averageRTT = float64(averageRTT)*DefaultSlidingMeanParameter +
				float64(rawProbe.RTT)*(1-DefaultSlidingMeanParameter)
		}

		p.averageRTT = atomic.NewDuration(time.Duration(averageRTT))
	}

	return ok
}

// GetProbes gets the probes list from struct probes
func (p *probes) GetProbes() *list.List {
	return p.items
}

// UpdatedAt gets the probe update time.
func (p *probes) UpdatedAt() *atomic.Time {
	return p.updatedAt
}

// AverageRTT gets the average RTT of probes.
func (p *probes) AverageRTT() *atomic.Duration {
	return p.averageRTT
}
