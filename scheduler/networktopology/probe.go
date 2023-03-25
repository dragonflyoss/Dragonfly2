package networktopology

import (
	"time"

	"d7y.io/dragonfly/v2/scheduler/resource"
)

type Probe struct {
	// Host metadata.
	Host *resource.Host
	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration
	// CreatedAt is the time to create probe.
	CreatedAt time.Time
}

// NewProbe creates a new probe instance.
func NewProbe(host *resource.Host, rtt time.Duration, createdAt time.Time) *Probe {
	return &Probe{
		Host:      host,
		RTT:       rtt,
		CreatedAt: createdAt,
	}
}
