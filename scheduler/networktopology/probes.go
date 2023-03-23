package networktopology

import (
	"container/list"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

// TODO(XZ): Here we need to design a timestamp measurement point.
var initTime = time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local)

type Probes interface {
	// LoadProbe return the latest probe.
	LoadProbe() (*probe, bool)

	// StoreProbe stores probe in probe list.
	StoreProbe(*probe)

	// GetProbes gets the probes list from struct probes
	GetProbes() *list.List

	// GetUpdatedAt gets the probe update time.
	GetUpdatedAt() time.Time

	// GetAverageRTT gets the average RTT of probes.
	GetAverageRTT() time.Duration
}

type probes struct {
	// Host metadata.
	Host *resource.Host
	// Probes is the array of probe.
	Probes *list.List
	// AverageRTT is the average round-trip time of probes.
	AverageRTT time.Duration
}

// NewProbes creates a new probe list instance.
func NewProbes(host *resource.Host) Probes {
	p := &probes{
		Host:       host,
		Probes:     list.New(),
		AverageRTT: time.Duration(0),
	}
	return p
}

func (p *probes) LoadProbe() (*probe, bool) {
	if p.Probes.Len() == 0 {
		return nil, false
	}
	return p.Probes.Back().Value.(*probe), true
}

// StoreProbe stores probe in probe list.
func (p *probes) StoreProbe(pro *probe) {
	if p.Probes.Len() == config.DefaultProbeQueueLength {
		front := p.Probes.Front()
		p.Probes.Remove(front)
	}
	p.Probes.PushBack(pro)

	//update AverageRtt by moving average method
	var averageRTT = float64(p.Probes.Front().Value.(*probe).RTT)
	for e := p.Probes.Front().Next(); e != nil; e = e.Next() {
		averageRTT = averageRTT*0.1 + float64(e.Value.(*probe).RTT)*0.9
	}
	p.AverageRTT = time.Duration(averageRTT)
}

// GetProbes gets the probes list from struct probes
func (p *probes) GetProbes() *list.List {
	return p.Probes
}

// GetUpdatedAt gets the probe update time.
func (p *probes) GetUpdatedAt() time.Time {
	if p.Probes.Len() != 0 {
		return p.Probes.Back().Value.(*probe).UpdatedAt
	}
	return initTime
}

// GetAverageRTT gets the average RTT of probes.
func (p *probes) GetAverageRTT() time.Duration {
	if p.Probes.Len() != 0 {
		return p.AverageRTT
	}
	return time.Duration(0)
}
