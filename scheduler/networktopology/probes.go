package networktopology

import (
	"container/list"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// TODO: Here we need to design a timestamp measurement point.
var InitTime = time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)

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
		UpdatedAt: UpdatedAt.AsTime(),
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

	//update AverageRtt
	var SumRTT time.Duration
	SumRTT = 0
	for e := p.Probes.Front(); e != nil; e = e.Next() {
		SumRTT += e.Value.(Probe).RTT
	}
	p.AverageRTT = SumRTT / time.Duration(p.Probes.Len())
}

// GetUpdatedAt gets the probe update time.
func (p *Probes) GetUpdatedAt() (time.Time, bool) {
	if p.Probes.Len() != 0 {
		return p.Probes.Back().Value.(*Probe).UpdatedAt, true
	}
	return InitTime, false
}
