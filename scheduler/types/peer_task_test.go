package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerTask_SetStatus(t *testing.T) {
	tests := []struct {
		name         string
		oldStatus    PeerTaskStatus
		newStatus    PeerTaskStatus
		swapStatus   PeerTaskStatus
		expectStatus PeerTaskStatus
	}{
		{
			name:         "accept change status 1",
			oldStatus:    PeerTaskStatusHealth,
			newStatus:    PeerTaskStatusBadNode,
			expectStatus: PeerTaskStatusBadNode,
		}, {
			name:         "accept change status 2",
			oldStatus:    PeerTaskStatusBadNode,
			newStatus:    PeerTaskStatusNodeGone,
			expectStatus: PeerTaskStatusNodeGone,
		}, {
			name:         "accept change status 3",
			oldStatus:    PeerTaskStatusNeedCheckNode,
			newStatus:    PeerTaskStatusNeedAdjustNode,
			expectStatus: PeerTaskStatusNeedAdjustNode,
		}, {
			name:         "accept change status 4",
			oldStatus:    PeerTaskStatusNodeGone,
			newStatus:    PeerTaskStatusDone,
			expectStatus: PeerTaskStatusDone,
		}, {
			name:         "reject change status 1",
			oldStatus:    PeerTaskStatusDone,
			newStatus:    PeerTaskStatusNodeGone,
			expectStatus: PeerTaskStatusDone,
		}, {
			name:         "reject change status 2",
			oldStatus:    PeerTaskStatusNeedChildren,
			newStatus:    PeerTaskStatusNodeGone,
			expectStatus: PeerTaskStatusNeedChildren,
		}, {
			name:         "accept change status health",
			oldStatus:    PeerTaskStatusNeedChildren,
			newStatus:    PeerTaskStatusHealth,
			swapStatus:   PeerTaskStatusNeedChildren,
			expectStatus: PeerTaskStatusHealth,
		}, {
			name:         "reject change status health",
			oldStatus:    PeerTaskStatusNeedChildren,
			newStatus:    PeerTaskStatusHealth,
			swapStatus:   PeerTaskStatusNodeGone,
			expectStatus: PeerTaskStatusNeedChildren,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockPT := NewPeerTask("test", nil, nil, func(task *PeerTask) {})
			mockPT.status = tc.oldStatus
			if tc.newStatus == PeerTaskStatusHealth {
				mockPT.SetNodeStatusHealth(tc.swapStatus)
			} else {
				mockPT.SetNodeStatus(tc.newStatus)
			}
			assert.New(t).Equal(tc.expectStatus, mockPT.GetNodeStatus())
		})
	}
}
