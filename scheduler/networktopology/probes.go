package networktopology

import (
	"container/list"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type Probes interface {
	// LoadProbe return the latest probe.
	LoadProbe() (*Probe, bool)

	// StoreProbe stores probe in probe list.
	StoreProbe(*Probe)

	// GetItems gets the probes list from struct probes
	GetItems() *list.List

	// GetUpdatedAt gets the probe update time.
	GetUpdatedAt() time.Time

	// GetAverageRTT gets the average RTT of probes.
	GetAverageRTT() time.Duration
}

type probes struct {
	// Host metadata.
	Host *resource.Host
	// Items is the list of probe.
	Items *list.List
	// AverageRTT is the average round-trip time of probes.
	AverageRTT time.Duration
}

// NewProbes creates a new probe list instance.
func NewProbes(host *resource.Host) Probes {
	p := &probes{
		Host:       host,
		Items:      list.New(),
		AverageRTT: time.Duration(0),
	}
	return p
}

func (p *probes) LoadProbe() (*Probe, bool) {
	if p.Items.Len() == 0 {
		return nil, false
	}
	return p.Items.Back().Value.(*Probe), true
}

// StoreProbe stores probe in probe list.
func (p *probes) StoreProbe(pro *Probe) {
	if p.Items.Len() == config.DefaultProbeQueueLength {
		front := p.Items.Front()
		p.Items.Remove(front)
	}
	p.Items.PushBack(pro)

	//update AverageRtt by moving average method
	var averageRTT = float64(p.Items.Front().Value.(*Probe).RTT)
	for e := p.Items.Front().Next(); e != nil; e = e.Next() {
		averageRTT = averageRTT*0.1 + float64(e.Value.(*Probe).RTT)*0.9
	}
	p.AverageRTT = time.Duration(averageRTT)
}

// GetItems gets the probes list from struct probes
func (p *probes) GetItems() *list.List {
	return p.Items
}

// GetUpdatedAt gets the probe update time.
func (p *probes) GetUpdatedAt() time.Time {
	if p.Items.Len() != 0 {
		return p.Items.Back().Value.(*Probe).UpdatedAt
	}
	return time.Time{}.UTC()
}

// GetAverageRTT gets the average RTT of probes.
func (p *probes) GetAverageRTT() time.Duration {
	if p.Items.Len() != 0 {
		return p.AverageRTT
	}
	return time.Duration(0)
}
