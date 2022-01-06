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
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/idgen"
)

var (
	mockPeerID = idgen.PeerID("127.0.0.1")
	// mockCDNPeerID = idgen.CDNPeerID("127.0.0.1")
)

func TestPeer_NewPeer(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		expect func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host)
	}{
		{
			name: "new peer",
			id:   mockPeerID,
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Empty(peer.Pieces)
				assert.Equal(peer.PieceCosts.Len(), uint(0))
				assert.Empty(peer.Stream)
				assert.Empty(peer.StopChannel)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Empty(peer.Parent)
				assert.Empty(peer.Children)
				assert.NotEqual(peer.CreateAt.Load(), 0)
				assert.NotEqual(peer.UpdateAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			tc.expect(t, NewPeer(tc.id, mockTask, mockHost), mockTask, mockHost)
		})
	}
}

func TestPeer_LoadChild(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, childID string)
	}{
		{
			name:    "load child",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, childID string) {
				assert := assert.New(t)
				child, ok := peer.LoadChild(childID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, childID)
			},
		},
		{
			name:    "child does not exist",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, childID string) {
				assert := assert.New(t)
				_, ok := peer.LoadChild(idgen.PeerID("0.0.0.0"))
				assert.Equal(ok, false)
			},
		},
		{
			name:    "load key is empty",
			childID: "",
			expect: func(t *testing.T, peer *Peer, childID string) {
				assert := assert.New(t)
				child, ok := peer.LoadChild(childID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, childID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskBackToSourceLimit, mockTaskURLMeta)
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreChild(mockChildPeer)
			tc.expect(t, peer, tc.childID)
		})
	}
}
