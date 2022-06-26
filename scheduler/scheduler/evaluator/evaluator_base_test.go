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

package evaluator

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

var (
	mockRawHost = &scheduler.PeerHost{
		Id:             idgen.HostID("hostname", 8003),
		Ip:             "127.0.0.1",
		RpcPort:        8003,
		DownPort:       8001,
		HostName:       "hostname",
		SecurityDomain: "security_domain",
		Location:       "location",
		Idc:            "idc",
		NetTopology:    "net_topology",
	}
	mockTaskURLMeta = &base.UrlMeta{
		Digest: "digest",
		Tag:    "tag",
		Range:  "range",
		Filter: "filter",
		Header: map[string]string{
			"content-length": "100",
		},
	}
	mockTaskBackToSourceLimit int32 = 200
	mockTaskURL                     = "http://example.com/foo"
	mockTaskID                      = idgen.TaskID(mockTaskURL, mockTaskURLMeta)
	mockPeerID                      = idgen.PeerID("127.0.0.1")
)

func TestEvaluatorBase_NewEvaluatorBase(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, e interface{})
	}{
		{
			name: "new evaluator base",
			expect: func(t *testing.T, e interface{}) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorBase")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewEvaluatorBase())
		})
	}
}

func TestEvaluatorBase_Evaluate(t *testing.T) {
	parentMockHost := resource.NewHost(mockRawHost)
	parentMockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
	childMockHost := resource.NewHost(mockRawHost)
	childMockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name            string
		parent          *resource.Peer
		child           *resource.Peer
		totalPieceCount int32
		mock            func(parent *resource.Peer, child *resource.Peer)
		expect          func(t *testing.T, score float64)
	}{
		{
			name:            "security domain is not the same",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), parentMockTask, parentMockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), childMockTask, childMockHost),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Host.SecurityDomain = "foo"
				child.Host.SecurityDomain = "bar"
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name:            "security domain is same",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), parentMockTask, parentMockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), childMockTask, childMockHost),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Host.SecurityDomain = "bac"
				child.Host.SecurityDomain = "bac"
				parent.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.9))
			},
		},
		{
			name:            "parent security domain is empty",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), parentMockTask, parentMockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), childMockTask, childMockHost),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Host.SecurityDomain = ""
				child.Host.SecurityDomain = "baz"
				parent.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.9))
			},
		},
		{
			name:            "child security domain is empty",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), parentMockTask, parentMockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), childMockTask, childMockHost),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Host.SecurityDomain = "baz"
				child.Host.SecurityDomain = ""
				parent.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.9))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eb := NewEvaluatorBase()
			tc.mock(tc.parent, tc.child)
			tc.expect(t, eb.Evaluate(tc.parent, tc.child, tc.totalPieceCount))
		})
	}
}

func TestEvaluatorBase_calculatePieceScore(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name            string
		parent          *resource.Peer
		child           *resource.Peer
		totalPieceCount int32
		mock            func(parent *resource.Peer, child *resource.Peer)
		expect          func(t *testing.T, score float64)
	}{
		{
			name:            "total piece count is zero and child pieces are empty",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name:            "total piece count is zero and parent pieces are empty",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				child.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(-1))
			},
		},
		{
			name:            "total piece count is zero and child pieces of length greater than parent pieces",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Pieces.Set(0)
				child.Pieces.Set(0)
				child.Pieces.Set(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(-1))
			},
		},
		{
			name:            "total piece count is zero and child pieces of length equal than parent pieces",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Pieces.Set(0)
				child.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name:            "total piece count is zero and parent pieces of length greater than child pieces",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Pieces.Set(0)
				parent.Pieces.Set(1)
				child.Pieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name:            "parent pieces are empty",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 10,
			mock:            func(parent *resource.Peer, child *resource.Peer) {},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name:            "parent pieces of length greater than zero",
			parent:          resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerID("127.0.0.1"), mockTask, mockHost),
			totalPieceCount: 10,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.Pieces.Set(0)
				parent.Pieces.Set(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.2))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(tc.parent, tc.child)
			tc.expect(t, calculatePieceScore(tc.parent, tc.child, tc.totalPieceCount))
		})
	}
}

func TestEvaluatorBase_calculateFreeLoadScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host, mockPeer *resource.Peer)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "host peers is not empty",
			mock: func(host *resource.Host, mockPeer *resource.Peer) {
				mockPeer.StoreParent(mockPeer)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.98))
			},
		},
		{
			name: "host peers is empty",
			mock: func(host *resource.Host, mockPeer *resource.Peer) {},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := resource.NewPeer(mockPeerID, mockTask, host)
			tc.mock(host, mockPeer)
			tc.expect(t, calculateFreeLoadScore(host))
		})
	}
}

func TestEvaluatorBase_calculateHostTypeAffinityScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *resource.Peer)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "host is normal peer",
			mock: func(peer *resource.Peer) {},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.5))
			},
		},
		{
			name: "host is seed peer but peer state is PeerStateSucceeded",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateSucceeded)
				peer.Host.Type = resource.HostTypeSuperSeed
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "host is seed peer but peer state is PeerStateRunning",
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.Host.Type = resource.HostTypeSuperSeed
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := resource.NewHost(mockRawHost)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer := resource.NewPeer(mockPeerID, mockTask, mockHost)
			tc.mock(peer)
			tc.expect(t, calculateHostTypeAffinityScore(peer))
		})
	}
}

func TestEvaluatorBase_calculateIDCAffinityScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(dstHost *resource.Host, srcHost *resource.Host)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.IDC = ""
				srcHost.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "dst host idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "src host idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				srcHost.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "idc is not the same",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.IDC = "foo"
				srcHost.IDC = "bar"
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "idc is the same",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.IDC = "example"
				srcHost.IDC = "example"
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dstHost := resource.NewHost(mockRawHost)
			srcHost := resource.NewHost(mockRawHost)
			tc.mock(dstHost, srcHost)
			tc.expect(t, calculateIDCAffinityScore(dstHost, srcHost))
		})
	}
}

func TestEvaluatorBase_calculateMultiElementAffinityScore(t *testing.T) {
	tests := []struct {
		name   string
		dst    string
		src    string
		expect func(t *testing.T, score float64)
	}{
		{
			name: "dst is empty and src is empty",
			dst:  "",
			src:  "",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "dst is empty",
			dst:  "",
			src:  "baz",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "src is empty",
			dst:  "bar",
			src:  "",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "has only one element and matches",
			dst:  "foo",
			src:  "foo",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name: "has only one element and does not match",
			dst:  "foo",
			src:  "bar",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "has multi element and match",
			dst:  "foo|bar",
			src:  "foo|bar",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name: "has multi element and does not match",
			dst:  "foo|bar",
			src:  "bar|foo",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "dst length is greater than src",
			dst:  "foo|bar|baz",
			src:  "foo|bar",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.4))
			},
		},
		{
			name: "src length is greater than dst",
			dst:  "foo|bar",
			src:  "foo|bar|baz",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.4))
			},
		},
		{
			name: "dst exceeds maximum length",
			dst:  "foo|bar|baz|bac|bae|baf",
			src:  "foo|bar|baz",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.6))
			},
		},
		{
			name: "src exceeds maximum length",
			dst:  "foo|bar|baz",
			src:  "foo|bar|baz|bac|bae|baf",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.6))
			},
		},
		{
			name: "dst and src both exceeds maximum length",
			dst:  "foo|bar|baz|bac|bae|baf",
			src:  "foo|bar|baz|bac|bae|baf",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, calculateMultiElementAffinityScore(tc.dst, tc.src))
		})
	}
}

func TestEvaluatorBase_IsBadNode(t *testing.T) {
	mockHost := resource.NewHost(mockRawHost)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, resource.WithBackToSourceLimit(mockTaskBackToSourceLimit))

	tests := []struct {
		name            string
		peer            *resource.Peer
		totalPieceCount int32
		mock            func(peer *resource.Peer)
		expect          func(t *testing.T, isBadNode bool)
	}{
		{
			name:            "peer state is PeerStateFailed",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateFailed)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "peer state is PeerStateLeave",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateLeave)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "peer state is PeerStatePending",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStatePending)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "peer state is PeerStateReceivedTiny",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateReceivedTiny)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "peer state is PeerStateReceivedSmall",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateReceivedSmall)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "peer state is PeerStateReceivedNormal",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateReceivedNormal)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "download costs does not meet the normal distribution and last cost is twenty times more than mean",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.AppendPieceCost(10)
				peer.AppendPieceCost(201)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "download costs does not meet the normal distribution and last cost is twenty times lower than mean",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				peer.AppendPieceCost(10)
				peer.AppendPieceCost(200)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.False(isBadNode)
			},
		},
		{
			name:            "download costs meet the normal distribution and last cost is too long",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				for i := 0; i < 30; i++ {
					peer.AppendPieceCost(int64(i))
				}
				peer.AppendPieceCost(50)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.True(isBadNode)
			},
		},
		{
			name:            "download costs meet the normal distribution and last cost is normal",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				for i := 0; i < 30; i++ {
					peer.AppendPieceCost(int64(i))
				}
				peer.AppendPieceCost(18)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.False(isBadNode)
			},
		},
		{
			name:            "download costs meet the normal distribution and last cost is too short",
			peer:            resource.NewPeer(mockPeerID, mockTask, mockHost),
			totalPieceCount: 1,
			mock: func(peer *resource.Peer) {
				peer.FSM.SetState(resource.PeerStateRunning)
				for i := 20; i < 50; i++ {
					peer.AppendPieceCost(int64(i))
				}
				peer.AppendPieceCost(0)
			},
			expect: func(t *testing.T, isBadNode bool) {
				assert := assert.New(t)
				assert.False(isBadNode)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eb := NewEvaluatorBase()
			tc.mock(tc.peer)
			tc.expect(t, eb.IsBadNode(tc.peer))
		})
	}
}
