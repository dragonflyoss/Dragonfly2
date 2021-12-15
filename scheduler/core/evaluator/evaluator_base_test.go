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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

const (
	cdnHostType    = "cdn"
	clientHostType = "client"
)

const (
	mockIP      = "127.0.0.1"
	mockTaskURL = "https://example.com"
)

type factor struct {
	hostType           string
	securityDomain     string
	idc                string
	location           string
	netTopology        string
	totalUploadLoad    uint32
	currentUploadLoad  uint32
	finishedPieceCount int32
	hostUUID           string
	taskPieceCount     int32
}

func TestEvaluatorEvaluate(t *testing.T) {
	tests := []struct {
		name   string
		parent *factor
		child  *factor
		expect func(t *testing.T, v float64)
	}{
		{
			name: "evaluate succeeded with cdn peer",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.6)))
			},
		},
		{
			name: "evaluate with different securityDomain",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foz",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0)))
			},
		},
		{
			name: "evaluate with empty securityDomain",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.6)))
			},
		},
		{
			name: "evaluate with different idc",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "baz",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.45)))
			},
		},
		{
			name: "evaluate with different location",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.59)))
			},
		},
		{
			name: "evaluate with empty location",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.55)))
			},
		},
		{
			name: "evaluate with excessive location",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e|f",
				netTopology:        "a|b|c|d|e",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e|f",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.6)))
			},
		},
		{
			name: "evaluate with different netTopology",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.58)))
			},
		},
		{
			name: "evaluate with empty netTopology",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.5)))
			},
		},
		{
			name: "evaluate with excessive netTopology",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 0,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.6)))
			},
		},
		{
			name: "evaluate with task piece count",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 20,
				hostUUID:           "example",
				taskPieceCount:     100,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				finishedPieceCount: 0,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(0.68)))
			},
		},
		{
			name: "evaluate without task piece count",
			parent: &factor{
				hostType:           cdnHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				totalUploadLoad:    100,
				currentUploadLoad:  0,
				finishedPieceCount: 20,
				hostUUID:           "example",
				taskPieceCount:     0,
			},
			child: &factor{
				hostType:           clientHostType,
				securityDomain:     "foo",
				idc:                "bar",
				location:           "a|b|c|d|e",
				netTopology:        "a|b|c|d|e|f",
				finishedPieceCount: 10,
				hostUUID:           "example",
			},
			expect: func(t *testing.T, v float64) {
				assert := assert.New(t)
				assert.True(mathutils.EqualFloat64(v, float64(4.6)))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := supervisor.NewTask(idgen.TaskID(mockTaskURL, nil), mockTaskURL, nil)

			parentHost := supervisor.NewClientHost(
				tc.parent.hostUUID, "", "", 0, 0,
				tc.parent.securityDomain,
				tc.parent.location,
				tc.parent.idc,
				supervisor.WithNetTopology(tc.parent.netTopology),
				supervisor.WithTotalUploadLoad(tc.parent.totalUploadLoad),
			)
			parentHost.CurrentUploadLoad.Store(tc.parent.currentUploadLoad)
			parent := supervisor.NewPeer(idgen.PeerID(mockIP), task, parentHost)
			parent.TotalPieceCount.Store(tc.parent.finishedPieceCount)

			childHost := supervisor.NewClientHost(
				tc.parent.hostUUID, "", "", 0, 0,
				tc.child.securityDomain,
				tc.child.location,
				tc.child.idc,
				supervisor.WithNetTopology(tc.child.netTopology),
			)
			child := supervisor.NewPeer(idgen.PeerID(mockIP), task, childHost)
			child.TotalPieceCount.Store(tc.child.finishedPieceCount)

			e := NewEvaluatorBase()
			tc.expect(t, e.Evaluate(parent, child, tc.parent.taskPieceCount))
		})
	}
}

func TestEvaluatorNeedAdjustParent(t *testing.T) {
	tests := []struct {
		name   string
		parent *factor
		child  *factor
		expect func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer)
	}{
		{
			name: "peer is CDN",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: cdnHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal(e.NeedAdjustParent(child), false)
			},
		},
		{
			name: "peer has no parent",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal(e.NeedAdjustParent(child), true)
			},
		},
		{
			name: "peer has done",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal(e.NeedAdjustParent(child), true)
			},
		},
		{
			name: "parent has leaved",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				parent.Leave()
				assert.Equal(e.NeedAdjustParent(child), true)
			},
		},
		{
			name: "empty costs",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				assert.Equal(e.NeedAdjustParent(child), false)
			},
		},
		{
			name: "costs are not normal distribution and peer should not be scheduler",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				child.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}...)
				assert.Equal(e.NeedAdjustParent(child), false)
			},
		},
		{
			name: "costs are not normal distribution and peer should be scheduler",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				child.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 23}...)
				assert.Equal(e.NeedAdjustParent(child), true)
			},
		},
		{
			name: "costs are normal distribution and peer should not be scheduler",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				child.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 5}...)
				assert.Equal(e.NeedAdjustParent(child), false)
			},
		},
		{
			name: "costs are normal distribution and peer should be scheduler",
			parent: &factor{
				hostType: clientHostType,
			},
			child: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, parent *supervisor.Peer, child *supervisor.Peer) {
				assert := assert.New(t)
				child.SetParent(parent)
				child.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15}...)
				assert.Equal(e.NeedAdjustParent(child), true)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := supervisor.NewTask(idgen.TaskID(mockTaskURL, nil), mockTaskURL, nil)

			parentHost := supervisor.NewClientHost(uuid.NewString(), "", "", 0, 0, "", "", "")
			parent := supervisor.NewPeer(idgen.PeerID(mockIP), task, parentHost)

			var child *supervisor.Peer
			if tc.child.hostType == cdnHostType {
				childHost := supervisor.NewCDNHost(uuid.NewString(), "", "", 0, 0, "", "", "")
				child = supervisor.NewPeer(idgen.CDNPeerID(mockIP), task, childHost)
			} else {
				childHost := supervisor.NewClientHost(uuid.NewString(), "", "", 0, 0, "", "", "")
				child = supervisor.NewPeer(idgen.PeerID(mockIP), task, childHost)
			}

			e := NewEvaluatorBase()
			tc.expect(t, e, parent, child)
		})
	}
}

func TestEvaluatorIsBadNode(t *testing.T) {
	tests := []struct {
		name   string
		peer   *factor
		expect func(t *testing.T, e Evaluator, peer *supervisor.Peer)
	}{
		{
			name: "peer is bad",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *supervisor.Peer) {
				assert := assert.New(t)
				peer.SetStatus(supervisor.PeerStatusFail)
				assert.Equal(e.IsBadNode(peer), true)
			},
		},
		{
			name: "peer is CDN",
			peer: &factor{
				hostType: cdnHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "empty costs",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *supervisor.Peer) {
				assert := assert.New(t)
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "costs length is available and peer is not bad node",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *supervisor.Peer) {
				assert := assert.New(t)
				peer.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}...)
				assert.Equal(e.IsBadNode(peer), false)
			},
		},
		{
			name: "costs length is available and peer is bad node",
			peer: &factor{
				hostType: clientHostType,
			},
			expect: func(t *testing.T, e Evaluator, peer *supervisor.Peer) {
				assert := assert.New(t)
				peer.SetPieceCosts([]int{1, 2, 3, 4, 5, 6, 7, 8, 181}...)
				assert.Equal(e.IsBadNode(peer), true)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := supervisor.NewTask(idgen.TaskID(mockTaskURL, nil), mockTaskURL, nil)

			var peer *supervisor.Peer
			if tc.peer.hostType == cdnHostType {
				childHost := supervisor.NewCDNHost(uuid.NewString(), "", "", 0, 0, "", "", "")
				peer = supervisor.NewPeer(idgen.CDNPeerID(mockIP), task, childHost)
			} else {
				childHost := supervisor.NewClientHost(uuid.NewString(), "", "", 0, 0, "", "", "")
				peer = supervisor.NewPeer(idgen.PeerID(mockIP), task, childHost)
			}

			e := NewEvaluatorBase()
			tc.expect(t, e, peer)
		})
	}
}
