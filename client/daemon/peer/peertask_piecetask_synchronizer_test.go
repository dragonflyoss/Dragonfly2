/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package peer

import (
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"

	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	"d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func Test_watchdog(t *testing.T) {
	ctrl := gomock.NewController(t)
	assert := testifyassert.New(t)

	var testCases = []struct {
		name    string
		timeout time.Duration
		ok      bool
	}{
		{
			name:    "watchdog ok",
			timeout: time.Millisecond,
			ok:      true,
		},
		{
			name:    "watchdog failed",
			timeout: time.Millisecond,
			ok:      false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			peer := &schedulerv1.PeerPacket_DestPeer{}
			pps := mocks.NewMockScheduler_ReportPieceResultClient(ctrl)
			watchdog := &synchronizerWatchdog{
				done:        make(chan struct{}),
				mainPeer:    atomic.Value{},
				syncSuccess: atomic.NewBool(false),
				peerTaskConductor: &peerTaskConductor{
					SugaredLoggerOnWith: logger.With(
						"peer", "test",
						"task", "test",
						"component", "PeerTask"),
					readyPieces:      NewBitmap(),
					peerPacketStream: pps,
				},
			}
			if tt.ok {
				watchdog.peerTaskConductor.readyPieces.Set(0)
			} else {
				pps.EXPECT().Send(gomock.Any()).DoAndReturn(func(pr *schedulerv1.PieceResult) error {
					assert.Equal(peer.PeerId, pr.DstPid)
					return nil
				})
			}
			watchdog.mainPeer.Store(peer)

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				watchdog.watch(tt.timeout)
				wg.Done()
			}()

			wg.Wait()
		})
	}
}

func Test_diffPeers(t *testing.T) {
	assert := testifyassert.New(t)

	var testCases = []struct {
		name         string
		workers      map[string]*pieceTaskSynchronizer
		peers        []*schedulerv1.PeerPacket_DestPeer
		peersToKeep  []*schedulerv1.PeerPacket_DestPeer
		peersToAdd   []*schedulerv1.PeerPacket_DestPeer
		peersToClose []string
	}{
		{
			name:    "add new peers with empty workers",
			workers: map[string]*pieceTaskSynchronizer{},
			peers: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToKeep: []*schedulerv1.PeerPacket_DestPeer{},
			peersToAdd: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToClose: []string{},
		},
		{
			name: "add new peers with some workers",
			workers: map[string]*pieceTaskSynchronizer{
				"peer-1": {},
			},
			peers: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToKeep: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-1",
				},
			},
			peersToAdd: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToClose: []string{},
		},
		{
			name: "keep peers",
			workers: map[string]*pieceTaskSynchronizer{
				"peer-0": {},
				"peer-1": {},
				"peer-2": {},
			},
			peers: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToKeep: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-0",
				},
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToAdd:   []*schedulerv1.PeerPacket_DestPeer{},
			peersToClose: []string{},
		},
		{
			name: "close peers",
			workers: map[string]*pieceTaskSynchronizer{
				"peer-0": {},
				"peer-1": {},
				"peer-2": {},
			},
			peers: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-3",
				},
				{
					PeerId: "peer-4",
				},
				{
					PeerId: "peer-5",
				},
			},
			peersToKeep: []*schedulerv1.PeerPacket_DestPeer{},
			peersToAdd: []*schedulerv1.PeerPacket_DestPeer{

				{
					PeerId: "peer-3",
				},
				{
					PeerId: "peer-4",
				},
				{
					PeerId: "peer-5",
				},
			},
			peersToClose: []string{
				"peer-0",
				"peer-1",
				"peer-2",
			},
		},
		{
			name: "mix peers",
			workers: map[string]*pieceTaskSynchronizer{
				"peer-0": {},
				"peer-1": {},
				"peer-2": {},
			},
			peers: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
				{
					PeerId: "peer-3",
				},
				{
					PeerId: "peer-4",
				},
				{
					PeerId: "peer-5",
				},
			},
			peersToKeep: []*schedulerv1.PeerPacket_DestPeer{
				{
					PeerId: "peer-1",
				},
				{
					PeerId: "peer-2",
				},
			},
			peersToAdd: []*schedulerv1.PeerPacket_DestPeer{

				{
					PeerId: "peer-3",
				},
				{
					PeerId: "peer-4",
				},
				{
					PeerId: "peer-5",
				},
			},
			peersToClose: []string{
				"peer-0",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			s := &pieceTaskSyncManager{
				workers: tt.workers,
			}
			peersToKeep, peersToAdd, peersToClose := s.diffPeers(tt.peers)
			assert.ElementsMatch(tt.peersToKeep, peersToKeep)
			assert.ElementsMatch(tt.peersToAdd, peersToAdd)
			assert.ElementsMatch(tt.peersToClose, peersToClose)
		})
	}
}
