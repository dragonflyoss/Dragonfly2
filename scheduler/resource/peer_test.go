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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/go-http-utils/headers"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
)

var (
	mockPeerID     = idgen.PeerID("127.0.0.1")
	mockSeedPeerID = idgen.SeedPeerID("127.0.0.1")
)

func TestPeer_NewPeer(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		options []PeerOption
		expect  func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host)
	}{
		{
			name: "new peer",
			id:   mockPeerID,
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Empty(peer.Pieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.Stream)
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
		{
			name:    "new peer with bizTag",
			id:      mockPeerID,
			options: []PeerOption{WithBizTag("foo")},
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Equal(peer.BizTag, "foo")
				assert.Empty(peer.Pieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.Stream)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			tc.expect(t, NewPeer(tc.id, mockTask, mockHost, tc.options...), mockTask, mockHost)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreChild(mockChildPeer)
			tc.expect(t, peer, tc.childID)
		})
	}
}

func TestPeer_StoreChild(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, childID string)
	}{
		{
			name:    "store child",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, childID string) {
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				child, ok = peer.LoadChild(childID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, childID)
				parent, ok = child.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, peer.ID)
			},
		},
		{
			name:    "store key is empty",
			childID: "",
			expect: func(t *testing.T, peer *Peer, childID string) {
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				child, ok = peer.LoadChild(childID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, childID)
				parent, ok = child.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, peer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreChild(mockChildPeer)
			tc.expect(t, peer, tc.childID)
		})
	}
}

func TestPeer_DeleteChild(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, mockChildPeer *Peer)
	}{
		{
			name:    "delete child",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				peer.DeleteChild(mockChildPeer.ID)
				assert := assert.New(t)

				var ok bool
				_, ok = peer.LoadChild(mockChildPeer.ID)
				assert.Equal(ok, false)
				_, ok = mockChildPeer.LoadParent()
				assert.Equal(ok, false)
			},
		},
		{
			name:    "delete key does not exist",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				peer.DeleteChild("")
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				child, ok = peer.LoadChild(mockChildPeer.ID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, mockChildPeer.ID)
				parent, ok = child.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, peer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreChild(mockChildPeer)
			tc.expect(t, peer, mockChildPeer)
		})
	}
}

func TestPeer_LoadParent(t *testing.T) {
	tests := []struct {
		name     string
		parentID string
		expect   func(t *testing.T, peer *Peer, parentID string)
	}{
		{
			name:     "load parent",
			parentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, parentID string) {
				assert := assert.New(t)
				parent, ok := peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, parentID)
			},
		},
		{
			name:     "parent does not exist",
			parentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, parentID string) {
				assert := assert.New(t)
				_, ok := peer.LoadChild(idgen.PeerID("0.0.0.0"))
				assert.Equal(ok, false)
			},
		},
		{
			name:     "load key is empty",
			parentID: "",
			expect: func(t *testing.T, peer *Peer, parentID string) {
				assert := assert.New(t)
				parent, ok := peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, parentID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockParentPeer := NewPeer(tc.parentID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreParent(mockParentPeer)
			tc.expect(t, peer, tc.parentID)
		})
	}
}

func TestPeer_StoreParent(t *testing.T) {
	tests := []struct {
		name     string
		parentID string
		expect   func(t *testing.T, peer *Peer, parentID string)
	}{
		{
			name:     "store parent",
			parentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, parentID string) {
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				parent, ok = peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, parentID)
				child, ok = parent.LoadChild(peer.ID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, peer.ID)
			},
		},
		{
			name:     "store key is empty",
			parentID: "",
			expect: func(t *testing.T, peer *Peer, parentID string) {
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				parent, ok = peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, parentID)
				child, ok = parent.LoadChild(peer.ID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, peer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockParentPeer := NewPeer(tc.parentID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			peer.StoreParent(mockParentPeer)
			tc.expect(t, peer, tc.parentID)
		})
	}
}

func TestPeer_DeleteParent(t *testing.T) {
	tests := []struct {
		name     string
		parentID string
		expect   func(t *testing.T, peer *Peer, mockParentPeer *Peer)
	}{
		{
			name:     "delete parent",
			parentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockParentPeer *Peer) {
				peer.StoreParent(mockParentPeer)
				peer.DeleteParent()
				assert := assert.New(t)

				var ok bool
				_, ok = peer.LoadParent()
				assert.Equal(ok, false)
				_, ok = mockParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name:     "parent does not exist",
			parentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockParentPeer *Peer) {
				peer.DeleteParent()
				assert := assert.New(t)

				var ok bool
				_, ok = peer.LoadParent()
				assert.Equal(ok, false)
				_, ok = mockParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockParentPeer := NewPeer(tc.parentID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer, mockParentPeer)
		})
	}
}

func TestPeer_ReplaceParent(t *testing.T) {
	tests := []struct {
		name        string
		oldParentID string
		newParentID string
		expect      func(t *testing.T, peer *Peer, mockOldParentPeer *Peer, mockNewParentPeer *Peer)
	}{
		{
			name:        "replace parent",
			oldParentID: idgen.PeerID("127.0.0.1"),
			newParentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockOldParentPeer *Peer, mockNewParentPeer *Peer) {
				peer.StoreParent(mockOldParentPeer)
				peer.ReplaceParent(mockNewParentPeer)
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				parent, ok = peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, mockNewParentPeer.ID)
				_, ok = mockOldParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, false)
				child, ok = mockNewParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, peer.ID)
			},
		},
		{
			name:        "old parent does not exist",
			oldParentID: idgen.PeerID("127.0.0.1"),
			newParentID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockOldParentPeer *Peer, mockNewParentPeer *Peer) {
				peer.ReplaceParent(mockNewParentPeer)
				assert := assert.New(t)

				var (
					parent *Peer
					child  *Peer
					ok     bool
				)
				parent, ok = peer.LoadParent()
				assert.Equal(ok, true)
				assert.Equal(parent.ID, mockNewParentPeer.ID)
				_, ok = mockOldParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, false)
				child, ok = mockNewParentPeer.LoadChild(peer.ID)
				assert.Equal(ok, true)
				assert.Equal(child.ID, peer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockOldParentPeer := NewPeer(tc.oldParentID, mockTask, mockHost)
			mockNewParentPeer := NewPeer(tc.newParentID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer, mockOldParentPeer, mockNewParentPeer)
		})
	}
}

func TestPeer_Depth(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, parent *Peer, seedPeerParent *Peer)
	}{
		{
			name: "there is only one node in the tree",
			expect: func(t *testing.T, peer *Peer, parent *Peer, seedPeerParent *Peer) {
				assert := assert.New(t)
				assert.Equal(peer.Depth(), 1)
			},
		},
		{
			name: "more than one node in the tree",
			expect: func(t *testing.T, peer *Peer, parent *Peer, seedPeerParent *Peer) {
				peer.StoreParent(parent)

				assert := assert.New(t)
				assert.Equal(peer.Depth(), 2)
			},
		},
		{
			name: "node parent is seed peer",
			expect: func(t *testing.T, peer *Peer, parent *Peer, seedPeerParent *Peer) {
				peer.StoreParent(seedPeerParent)

				assert := assert.New(t)
				assert.Equal(peer.Depth(), 2)
			},
		},
		{
			name: "node parent is itself",
			expect: func(t *testing.T, peer *Peer, parent *Peer, seedPeerParent *Peer) {
				peer.StoreParent(peer)

				assert := assert.New(t)
				assert.Equal(peer.Depth(), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockSeedHost := NewHost(mockRawSeedHost, WithHostType(HostTypeSuperSeed))
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)
			parent := NewPeer(idgen.PeerID("127.0.0.2"), mockTask, mockHost)
			seedPeerParent := NewPeer(mockSeedPeerID, mockTask, mockSeedHost)

			tc.expect(t, peer, parent, seedPeerParent)
		})
	}
}

func TestPeer_Ancestors(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, mockChildPeer *Peer)
	}{
		{
			name:    "parent is ancestor",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(mockChildPeer)
				assert.Equal(len(mockChildPeer.Ancestors()), 1)
				assert.EqualValues(mockChildPeer.Ancestors(), []string{peer.ID})
			},
		},
		{
			name:    "child has no parent",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(len(mockChildPeer.Ancestors()), 0)
				assert.EqualValues(mockChildPeer.Ancestors(), []string(nil))
			},
		},
		{
			name:    "infinite loop",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(peer)
				assert.Equal(len(peer.Ancestors()), 0)
				assert.Equal(peer.Ancestors(), []string(nil))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer, mockChildPeer)
		})
	}
}

func TestPeer_IsDescendant(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, mockChildPeer *Peer)
	}{
		{
			name:    "child is descendant",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(mockChildPeer)
				assert.Equal(mockChildPeer.IsDescendant(peer), true)
			},
		},
		{
			name:    "child is not descendant",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(mockChildPeer)
				assert.Equal(mockChildPeer.IsDescendant(mockChildPeer), false)
			},
		},
		{
			name:    "parent has no children",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(mockChildPeer.IsDescendant(peer), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer, mockChildPeer)
		})
	}
}

func TestPeer_IsAncestor(t *testing.T) {
	tests := []struct {
		name    string
		childID string
		expect  func(t *testing.T, peer *Peer, mockChildPeer *Peer)
	}{
		{
			name:    "parent is ancestor",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(mockChildPeer)
				assert.Equal(peer.IsAncestor(mockChildPeer), true)
			},
		},
		{
			name:    "parent is not ancestor",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				peer.StoreChild(mockChildPeer)
				assert.Equal(peer.IsDescendant(peer), false)
			},
		},
		{
			name:    "child has no parent",
			childID: idgen.PeerID("127.0.0.1"),
			expect: func(t *testing.T, peer *Peer, mockChildPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(peer.IsDescendant(mockChildPeer), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockChildPeer := NewPeer(tc.childID, mockTask, mockHost)
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer, mockChildPeer)
		})
	}
}

func TestPeer_AppendPieceCost(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer)
	}{
		{
			name: "append piece cost",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.AppendPieceCost(1)
				costs := peer.PieceCosts()
				assert.Equal(costs[0], int64(1))
			},
		},
		{
			name: "piece costs slice is empty",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				costs := peer.PieceCosts()
				assert.Equal(len(costs), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer)
		})
	}
}

func TestPeer_PieceCosts(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer)
	}{
		{
			name: "piece costs slice is not empty",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.AppendPieceCost(1)
				costs := peer.PieceCosts()
				assert.Equal(costs[0], int64(1))
			},
		},
		{
			name: "piece costs slice is empty",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				costs := peer.PieceCosts()
				assert.Equal(len(costs), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)

			tc.expect(t, peer)
		})
	}
}

func TestPeer_LoadStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "load stream",
			expect: func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreStream(stream)
				newStream, ok := peer.LoadStream()
				assert.Equal(ok, true)
				assert.EqualValues(newStream, stream)
			},
		},
		{
			name: "stream does not exist",
			expect: func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				_, ok := peer.LoadStream()
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_StoreStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "store stream",
			expect: func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreStream(stream)
				newStream, ok := peer.LoadStream()
				assert.Equal(ok, true)
				assert.EqualValues(newStream, stream)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_DeleteStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "delete stream",
			expect: func(t *testing.T, peer *Peer, stream scheduler.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreStream(stream)
				peer.DeleteStream()
				_, ok := peer.LoadStream()
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := NewPeer(mockPeerID, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_DownloadTinyFile(t *testing.T) {
	testData := []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" +
		"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	newServer := func(t *testing.T, getPeer func() *Peer) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer := getPeer()
			assert := assert.New(t)
			assert.NotNil(peer)
			assert.Equal(r.URL.Path, fmt.Sprintf("/download/%s/%s", peer.Task.ID[:3], peer.Task.ID))
			assert.Equal(r.URL.RawQuery, fmt.Sprintf("peerId=%s", peer.ID))

			rgs, err := clientutil.ParseRange(r.Header.Get(headers.Range), 128)
			assert.Nil(err)
			assert.Equal(1, len(rgs))
			rg := rgs[0]

			w.WriteHeader(http.StatusPartialContent)
			n, err := w.Write(testData[rg.Start : rg.Start+rg.Length])
			assert.Nil(err)
			assert.Equal(int64(n), rg.Length)
		}))
	}
	tests := []struct {
		name      string
		newServer func(t *testing.T, getPeer func() *Peer) *httptest.Server
		expect    func(t *testing.T, peer *Peer)
	}{
		{
			name: "download tiny file - 32",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ContentLength.Store(32)
				data, err := peer.DownloadTinyFile()
				assert.NoError(err)
				assert.Equal(testData[:32], data)
			},
		},
		{
			name: "download tiny file - 128",
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ContentLength.Store(32)
				data, err := peer.DownloadTinyFile()
				assert.NoError(err)
				assert.Equal(testData[:32], data)
			},
		},
		{
			name: "download tiny file failed because of http status code",
			newServer: func(t *testing.T, getPeer func() *Peer) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
			},
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ID = "foobar"
				_, err := peer.DownloadTinyFile()
				assert.EqualError(err, fmt.Sprintf("http://%s:%d/download/%s/%s?peerId=%s: 404 Not Found",
					peer.Host.IP, peer.Host.DownloadPort, peer.Task.ID[:3], peer.Task.ID, peer.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var peer *Peer
			if tc.newServer == nil {
				tc.newServer = newServer
			}
			s := tc.newServer(t, func() *Peer {
				return peer
			})
			defer s.Close()
			url, err := url.Parse(s.URL)
			if err != nil {
				t.Fatal(err)
			}

			ip, rawPort, err := net.SplitHostPort(url.Host)
			if err != nil {
				t.Fatal(err)
			}

			port, err := strconv.ParseInt(rawPort, 10, 32)
			if err != nil {
				t.Fatal(err)
			}

			mockRawHost.Ip = ip
			mockRawHost.DownPort = int32(port)
			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer = NewPeer(mockPeerID, mockTask, mockHost)
			tc.expect(t, peer)
		})
	}
}
