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

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/idgen"
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
				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
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
				_, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
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
				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
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

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
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
				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
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
				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
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

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
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
				peer, loaded := peerManager.LoadOrStore(mockPeer)
				assert.Equal(loaded, true)
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
				peer, loaded := peerManager.LoadOrStore(mockPeer)
				assert.Equal(loaded, false)
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

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
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
				_, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
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
				_, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
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
		name     string
		gcConfig *config.GCConfig
		mock     func(m *gc.MockGCMockRecorder)
		expect   func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer)
	}{
		{
			name: "peer leave",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              1 * time.Microsecond,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)
			},
		},
		{
			name: "peer download piece timeout and peer state is PeerStateRunning",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 1 * time.Microsecond,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              5 * time.Minute,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateRunning)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)

				err = peerManager.RunGC()
				assert.NoError(err)

				_, loaded = peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "peer download piece timeout and peer state is PeerStateBackToSource",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 1 * time.Microsecond,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              5 * time.Minute,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateBackToSource)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)

				err = peerManager.RunGC()
				assert.NoError(err)

				_, loaded = peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "peer reclaimed with peer ttl",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              1 * time.Microsecond,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)

				err = peerManager.RunGC()
				assert.NoError(err)

				_, loaded = peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "peer reclaimed with host ttl",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              10 * time.Second,
				HostTTL:              1 * time.Microsecond,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)

				err = peerManager.RunGC()
				assert.NoError(err)

				_, loaded = peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "peer state is PeerStateFailed",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              1 * time.Microsecond,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateFailed)
				err := peerManager.RunGC()
				assert.NoError(err)

				peer, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, true)
				assert.Equal(peer.FSM.Current(), PeerStateLeave)
			},
		},
		{
			name: "peer gets degree failed",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              1 * time.Hour,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				mockPeer.Task.DeletePeer(mockPeer.ID)

				err := peerManager.RunGC()
				assert.NoError(err)

				_, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "peer reclaimed with PeerCountLimitForTask",
			gcConfig: &config.GCConfig{
				PieceDownloadTimeout: 5 * time.Minute,
				PeerGCInterval:       1 * time.Second,
				PeerTTL:              1 * time.Hour,
				HostTTL:              10 * time.Second,
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, peerManager PeerManager, mockHost *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peerManager.Store(mockPeer)
				mockPeer.FSM.SetState(PeerStateSucceeded)
				for i := 0; i < PeerCountLimitForTask+1; i++ {
					peer := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost)
					mockPeer.Task.StorePeer(peer)
				}

				err := peerManager.RunGC()
				assert.NoError(err)

				_, loaded := peerManager.Load(mockPeer.ID)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			peerManager, err := newPeerManager(tc.gcConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, peerManager, mockHost, mockTask, mockPeer)
		})
	}
}
