package networktopology

import (
	"container/list"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

// TODO(XZ): Here we need to design a timestamp measurement point.
var initTime = time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)

type Probe struct {
	// Host metadata.
	Host *resource.Host
	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration
	// Probe update time.
	UpdatedAt time.Time
}

// NewProbe creates a new probe instance.
func NewProbe(host *resource.Host, RTT *durationpb.Duration, UpdatedAt *timestamppb.Timestamp) *Probe {
	p := &Probe{
		Host:      host,
		RTT:       RTT.AsDuration(),
		UpdatedAt: UpdatedAt.AsTime().Local(),
	}
	return p
}

type Probes struct {
	// Host metadata.
	Host *resource.Host
	// Probes is the array of probe.
	Probes *list.List
	// AverageRTT is the average round-trip time of probes.
	AverageRTT time.Duration
}

// NewProbes creates a new probe list instance.
func NewProbes(host *resource.Host) *Probes {
	p := &Probes{
		Host:       host,
		Probes:     list.New(),
		AverageRTT: time.Duration(0),
	}
	return p
}

// LoadProbe return the latest probe.
func (p *Probes) LoadProbe() (*Probe, bool) {
	if p.Probes.Len() == 0 {
		return nil, false
	}
	return p.Probes.Back().Value.(*Probe), true
}

// StoreProbe stores probe in probe list.
func (p *Probes) StoreProbe(probe *Probe) {
	if p.Probes.Len() == config.DefaultProbeQueueLength {
		front := p.Probes.Front()
		p.Probes.Remove(front)
	} else {
		p.Probes.PushBack(probe)
	}

	//update AverageRtt by moving average method
	var averageRTT = float64(p.Probes.Front().Value.(*Probe).RTT)
	for e := p.Probes.Front().Next(); e != nil; e = e.Next() {
		averageRTT += averageRTT*0.1 + float64(e.Value.(*Probe).RTT)*0.9
	}
	p.AverageRTT = time.Duration(averageRTT)
}

// GetUpdatedAt gets the probe update time.
func (p *Probes) GetUpdatedAt() (time.Time, bool) {
	if p.Probes.Len() != 0 {
		return p.Probes.Back().Value.(*Probe).UpdatedAt, true
	}
	return initTime, false
}

// GetAverageRTT gets the average RTT of probes.
func (p *Probes) GetAverageRTT() (time.Duration, bool) {
	if p.Probes.Len() != 0 {
		return p.AverageRTT, true
	}
	return time.Duration(0), false
}
