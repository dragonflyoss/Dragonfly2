/*
 *     Copyright 2020 The Dragonfly Authors
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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockPeerGCConfig = &config.GCConfig{
		PeerGCInterval: 1 * time.Second,
		PeerTTL:        1 * time.Microsecond,
	}
)

func TestPeerManager_newPeerManager(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, err error)
	}{
		{
			name: "new peer manager",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(peerManager).Elem().Name(), "peerManager")
			},
		},
		{
			name: "new peer manager failed because of gc error",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			tc.expect(t, peerManager, err)
		})
	}
}

func TestPeerManager_Load(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, mockPeer *Peer)
	}{
		{
			name: "load peer",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
		{
			name: "peer does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				_, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "load key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.ID = ""
				peerManager.Store(mockPeer)
				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockPeer)
		})
	}
}

func TestPeerManager_Store(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, mockPeer *Peer)
	}{
		{
			name: "store peer",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
		{
			name: "store key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.ID = ""
				peerManager.Store(mockPeer)
				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockPeer)
		})
	}
}

func TestPeerManager_LoadOrStore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, mockPeer *Peer)
	}{
		{
			name: "load peer exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				peer, ok := peerManager.LoadOrStore(mockPeer)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
		{
			name: "load peer does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peer, ok := peerManager.LoadOrStore(mockPeer)
				assert.Equal(ok, false)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockPeer)
		})
	}
}

func TestPeerManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, mockPeer *Peer)
	}{
		{
			name: "delete peer",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				peerManager.Delete(mockPeer.ID)
				_, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "delete key does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.ID = ""
				peerManager.Store(mockPeer)
				peerManager.Delete(mockPeer.ID)
				_, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockPeer)
		})
	}
}

func TestPeerManager_RunGC(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, peerManager PeerManager, mockPeer *Peer)
	}{
		{
			name: "peer leave",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)
			},
		},
		{
			name: "peer reclaimed",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)

				err = peerManager.RunGC()
				assert.NoError(err)

				_, ok = peerManager.Load(mockPeer.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "peer has children",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				mockPeer.StoreChild(mockPeer)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.FSM.Current(), PeerStateSucceeded)
			},
		},
		{
			name: "peer state is PeerStatePending",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStatePending)
				mockPeer.StoreChild(mockPeer)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, ok := peerManager.Load(mockPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			peerManager, err := newPeerManager(mockPeerGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockPeer)
		})
	}
}
