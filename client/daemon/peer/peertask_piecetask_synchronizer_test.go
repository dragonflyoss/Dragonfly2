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

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
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
			peer := &scheduler.PeerPacket_DestPeer{}
			pps := mocks.NewMockPeerPacketStream(ctrl)
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
				pps.EXPECT().Send(gomock.Any()).DoAndReturn(func(pr *scheduler.PieceResult) error {
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
