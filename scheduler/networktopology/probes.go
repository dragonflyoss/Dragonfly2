package networktopology

import (
	"container/list"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

const DefaultMovingAverageValue = 0.1

type Probes interface {
	// LoadProbe return the latest probe.
	LoadProbe() (*Probe, bool)

	// StoreProbe stores probe in probe list.
	StoreProbe(*Probe) bool

	// GetProbes gets the probes list from struct probes
	GetProbes() *list.List

	// UpdatedAt gets the probe update time.
	UpdatedAt() time.Time

	// AverageRTT gets the average RTT of probes.
	AverageRTT() time.Duration
}

type probes struct {
	// config is scheduler configuration.
	config *config.Config

	// host metadata.
	host *resource.Host

	// items is the list of probe.
	items *list.List

	// averageRTT is the average round-trip time of probes.
	averageRTT time.Duration

	// updatedAt is the update time to store probe.
	updatedAt time.Time
}

// NewProbes creates a new probe list instance.
func NewProbes(cfg *config.Config, host *resource.Host) Probes {
	return &probes{
		config:     cfg,
		host:       host,
		items:      list.New(),
		averageRTT: time.Duration(0),
		updatedAt:  time.Time{},
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
func (p *probes) StoreProbe(pro *Probe) bool {
	if p.items.Len() == p.config.NetworkTopology.Probe.QueueLength {
		front := p.items.Front()
		p.items.Remove(front)
	}
	p.items.PushBack(pro)

	//update updatedAt
	p.updatedAt = pro.CreatedAt

	//update AverageRtt by moving average method
	front, ok := p.items.Front().Value.(*Probe)
	if ok {
		averageRTT := float64(front.RTT)
		for e := p.items.Front().Next(); e != nil; e = e.Next() {
			rawProbe, loaded := e.Value.(*Probe)
			if !loaded {
				return loaded
			}

			averageRTT = float64(averageRTT)*DefaultMovingAverageValue +
				float64(rawProbe.RTT)*(1-DefaultMovingAverageValue)
		}

		p.averageRTT = time.Duration(averageRTT)
	}

	return ok
}

// GetProbes gets the probes list from struct probes
func (p *probes) GetProbes() *list.List {
	return p.items
}

// UpdatedAt gets the probe update time.
func (p *probes) UpdatedAt() time.Time {
	return p.updatedAt
}

// AverageRTT gets the average RTT of probes.
func (p *probes) AverageRTT() time.Duration {
	return p.averageRTT
}
