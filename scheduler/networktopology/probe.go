package networktopology

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"d7y.io/dragonfly/v2/scheduler/resource"
)

type probe struct {
	// Host metadata.
	Host *resource.Host
	// RTT is the round-trip time sent via this pinger.
	RTT time.Duration
	// Probe update time.
	UpdatedAt time.Time
}

// NewProbe creates a new probe instance.
func NewProbe(host *resource.Host, RTT *durationpb.Duration, UpdatedAt *timestamppb.Timestamp) *probe {
	p := &probe{
		Host:      host,
		RTT:       RTT.AsDuration(),
		UpdatedAt: UpdatedAt.AsTime().Local(),
	}
	return p
}
