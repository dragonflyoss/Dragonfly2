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
	// Queue is the list of probe.
	Queue *list.List
	// AverageRTT is the average round-trip time of probes.
	AverageRTT time.Duration
}

// NewProbes creates a new probe list instance.
func NewProbes(host *resource.Host) Probes {
	p := &probes{
		Host:       host,
		Queue:      list.New(),
		AverageRTT: time.Duration(0),
	}
	return p
}

func (p *probes) LoadProbe() (*Probe, bool) {
	if p.Queue.Len() == 0 {
		return nil, false
	}
	return p.Queue.Back().Value.(*Probe), true
}

// StoreProbe stores probe in probe list.
func (p *probes) StoreProbe(pro *Probe) {
	if p.Queue.Len() == config.DefaultProbeQueueLength {
		front := p.Queue.Front()
		p.Queue.Remove(front)
	}
	p.Queue.PushBack(pro)

	//update AverageRtt by moving average method
	var averageRTT = float64(p.Queue.Front().Value.(*Probe).RTT)
	for e := p.Queue.Front().Next(); e != nil; e = e.Next() {
		averageRTT = averageRTT*0.1 + float64(e.Value.(*Probe).RTT)*0.9
	}
	p.AverageRTT = time.Duration(averageRTT)
}

// GetProbes gets the probes list from struct probes
func (p *probes) GetProbes() *list.List {
	return p.Queue
}

// GetUpdatedAt gets the probe update time.
func (p *probes) GetUpdatedAt() time.Time {
	if p.Queue.Len() != 0 {
		return p.Queue.Back().Value.(*Probe).UpdatedAt
	}
	return time.Time{}.UTC()
}

// GetAverageRTT gets the average RTT of probes.
func (p *probes) GetAverageRTT() time.Duration {
	if p.Queue.Len() != 0 {
		return p.AverageRTT
	}
	return time.Duration(0)
}
