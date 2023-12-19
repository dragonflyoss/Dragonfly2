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

package resource

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
)

func TestSeedPeer_newSeedPeer(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s SeedPeer)
	}{
		{
			name: "new seed peer",
			expect: func(t *testing.T, s SeedPeer) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "seedPeer")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			hostManager := NewMockHostManager(ctl)
			peerManager := NewMockPeerManager(ctl)
			client := NewMockSeedPeerClient(ctl)

			tc.expect(t, newSeedPeer(mockConfig, client, peerManager, hostManager))
		})
	}
}

func TestSeedPeer_TriggerDownloadTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mc *MockSeedPeerClientMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "trigger download task failed",
			mock: func(mc *MockSeedPeerClientMockRecorder) {
				mc.TriggerDownloadTask(gomock.Any(), gomock.Any()).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "trigger download task scuccess",
			mock: func(mc *MockSeedPeerClientMockRecorder) {
				mc.TriggerDownloadTask(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			hostManager := NewMockHostManager(ctl)
			peerManager := NewMockPeerManager(ctl)
			client := NewMockSeedPeerClient(ctl)
			tc.mock(client.EXPECT())

			seedPeer := newSeedPeer(mockConfig, client, peerManager, hostManager)
			tc.expect(t, seedPeer.TriggerDownloadTask(context.Background(), &dfdaemonv2.TriggerDownloadTaskRequest{}))
		})
	}
}

func TestSeedPeer_TriggerTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mc *MockSeedPeerClientMockRecorder)
		expect func(t *testing.T, peer *Peer, result *schedulerv1.PeerResult, err error)
	}{
		{
			name: "start obtain seed stream failed",
			mock: func(mc *MockSeedPeerClientMockRecorder) {
				mc.ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *Peer, result *schedulerv1.PeerResult, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			hostManager := NewMockHostManager(ctl)
			peerManager := NewMockPeerManager(ctl)
			client := NewMockSeedPeerClient(ctl)
			tc.mock(client.EXPECT())

			seedPeer := newSeedPeer(mockConfig, client, peerManager, hostManager)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer, result, err := seedPeer.TriggerTask(context.Background(), nil, mockTask)
			tc.expect(t, peer, result, err)
		})
	}
}
