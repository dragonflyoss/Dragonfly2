/*
 *     Copyright 2024 The Dragonfly Authors
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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	networktopologymocks "d7y.io/dragonfly/v2/scheduler/networktopology/mocks"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

func TestEvaluatorNetworkTopology_newEvaluatorNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, e any)
	}{
		{
			name: "new evaluator commonv1",
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorNetworkTopology")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			tc.expect(t, newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology)))
		})
	}
}

func TestEvaluatorNetworkTopology_EvaluateParents(t *testing.T) {
	tests := []struct {
		name            string
		parents         []*resource.Peer
		child           *resource.Peer
		totalPieceCount int32
		mock            func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder)
		expect          func(t *testing.T, parents []*resource.Peer)
	}{
		{
			name:    "parents is empty",
			parents: []*resource.Peer{},
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
			},
			expect: func(t *testing.T, parents []*resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(parents), 0)
			},
		},
		{
			name: "evaluate single parent",
			parents: []*resource.Peer{
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			},
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
			},
			expect: func(t *testing.T, parents []*resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(parents), 1)
				assert.Equal(parents[0].Task.ID, mockTaskID)
				assert.Equal(parents[0].Host.ID, mockRawSeedHost.ID)
			},
		},
		{
			name: "evaluate parents with free upload count",
			parents: []*resource.Peer{
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bar", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"baz", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bac", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bae", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			},
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
				parents[1].Host.ConcurrentUploadCount.Add(4)
				parents[2].Host.ConcurrentUploadCount.Add(3)
				parents[3].Host.ConcurrentUploadCount.Add(2)
				parents[4].Host.ConcurrentUploadCount.Add(1)
				mn.Probes(gomock.Any(), gomock.Any()).Return(p).Times(20)
				mp.AverageRTT().Return(1*time.Second, nil).Times(20)
			},
			expect: func(t *testing.T, parents []*resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(parents), 5)
				assert.Equal(parents[0].Host.ID, mockRawSeedHost.ID)
				assert.Equal(parents[1].Host.ID, "bae")
				assert.Equal(parents[2].Host.ID, "bac")
				assert.Equal(parents[3].Host.ID, "baz")
				assert.Equal(parents[4].Host.ID, "bar")
			},
		},
		{
			name: "evaluate parents with pieces",
			parents: []*resource.Peer{
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bar", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"baz", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bac", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bae", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			},
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
				parents[1].FinishedPieces.Set(0)
				parents[2].FinishedPieces.Set(0).Set(1)
				parents[3].FinishedPieces.Set(0).Set(1).Set(2)
				parents[4].FinishedPieces.Set(0).Set(1).Set(2).Set(3)
				mn.Probes(gomock.Any(), gomock.Any()).Return(p).Times(20)
				mp.AverageRTT().Return(1*time.Second, nil).Times(20)
			},
			expect: func(t *testing.T, parents []*resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(parents), 5)
				assert.Equal(parents[0].Host.ID, "bae")
				assert.Equal(parents[1].Host.ID, "bac")
				assert.Equal(parents[2].Host.ID, "baz")
				assert.Equal(parents[3].Host.ID, "bar")
				assert.Equal(parents[4].Host.ID, mockRawSeedHost.ID)
			},
		},
		{
			name: "evaluate single parent with networkTopology",
			parents: []*resource.Peer{
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
				resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
					resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
					resource.NewHost(
						"bar", mockRawSeedHost.IP, mockRawSeedHost.Hostname,
						mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			},
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parents []*resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
				mn.Probes(parents[0].ID, child.ID).Return(p)
				mp.AverageRTT().Return(100*time.Millisecond, nil)
				mn.Probes(parents[1].ID, child.ID).Return(p)
				mp.AverageRTT().Return(200*time.Millisecond, nil)
			},
			expect: func(t *testing.T, parents []*resource.Peer) {
				assert := assert.New(t)
				assert.Equal(len(parents), 2)
				assert.Equal(parents[0].Host.ID, "bar")
				assert.Equal(parents[1].Host.ID, mockRawSeedHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			mockProbe := networktopologymocks.NewMockProbes(ctl)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(tc.parents, tc.child, mockProbe, mockNetworkTopology.EXPECT(), mockProbe.EXPECT())
			tc.expect(t, e.EvaluateParents(tc.parents, tc.child, tc.totalPieceCount))
		})
	}
}

func TestEvaluatorNetworkTopology_evaluate(t *testing.T) {
	tests := []struct {
		name            string
		parent          *resource.Peer
		child           *resource.Peer
		totalPieceCount int32
		mock            func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mp *networktopologymocks.MockProbesMockRecorder, mn *networktopologymocks.MockNetworkTopologyMockRecorder)
		expect          func(t *testing.T, score float64)
	}{
		{
			name: "evaluate parent",
			parent: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mp *networktopologymocks.MockProbesMockRecorder, mn *networktopologymocks.MockNetworkTopologyMockRecorder) {
				mn.Probes(parent.ID, child.ID).Return(p)
				mp.AverageRTT().Return(500*time.Millisecond, nil)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.41))
			},
		},
		{
			name: "evaluate parent with pieces",
			parent: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			totalPieceCount: 1,
			mock: func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mp *networktopologymocks.MockProbesMockRecorder, mn *networktopologymocks.MockNetworkTopologyMockRecorder) {
				parent.FinishedPieces.Set(0)
				mn.Probes(parent.ID, child.ID).Return(p)
				mp.AverageRTT().Return(1000*time.Millisecond, nil)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.55))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			mockProbe := networktopologymocks.NewMockProbes(ctl)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(tc.parent, tc.child, mockProbe, mockProbe.EXPECT(), mockNetworkTopology.EXPECT())
			tc.expect(t, e.(*evaluatorNetworkTopology).evaluate(tc.parent, tc.child, tc.totalPieceCount))
		})
	}
}

func TestEvaluatorNetworkTopology_calculatePieceScore(t *testing.T) {
	mockHost := resource.NewHost(
		mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
		mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
	mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))

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
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.FinishedPieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name:            "total piece count is zero and parent pieces are empty",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				child.FinishedPieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(-1))
			},
		},
		{
			name:            "total piece count is zero and child pieces of length greater than parent pieces",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.FinishedPieces.Set(0)
				child.FinishedPieces.Set(0)
				child.FinishedPieces.Set(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(-1))
			},
		},
		{
			name:            "total piece count is zero and child pieces of length equal than parent pieces",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.FinishedPieces.Set(0)
				child.FinishedPieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name:            "total piece count is zero and parent pieces of length greater than child pieces",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 0,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.FinishedPieces.Set(0)
				parent.FinishedPieces.Set(1)
				child.FinishedPieces.Set(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name:            "parent pieces are empty",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 10,
			mock:            func(parent *resource.Peer, child *resource.Peer) {},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name:            "parent pieces of length greater than zero",
			parent:          resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			child:           resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, mockTask, mockHost),
			totalPieceCount: 10,
			mock: func(parent *resource.Peer, child *resource.Peer) {
				parent.FinishedPieces.Set(0)
				parent.FinishedPieces.Set(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.2))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(tc.parent, tc.child)
			tc.expect(t, e.(*evaluatorNetworkTopology).calculatePieceScore(tc.parent, tc.child, tc.totalPieceCount))
		})
	}
}

func TestEvaluatorNetworkTopology_calculatehostUploadSuccessScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "UploadFailedCount is larger than UploadCount",
			mock: func(host *resource.Host) {
				host.UploadCount.Add(1)
				host.UploadFailedCount.Add(2)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "UploadFailedCount and UploadCount is zero",
			mock: func(host *resource.Host) {
				host.UploadCount.Add(0)
				host.UploadFailedCount.Add(0)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
		{
			name: "UploadCount is larger than UploadFailedCount",
			mock: func(host *resource.Host) {
				host.UploadCount.Add(2)
				host.UploadFailedCount.Add(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.5))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, host)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(host)
			tc.expect(t, e.(*evaluatorNetworkTopology).calculateParentHostUploadSuccessScore(mockPeer))
		})
	}
}

func TestEvaluatorNetworkTopology_calculateFreeUploadScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(host *resource.Host, mockPeer *resource.Peer)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "host peers is not empty",
			mock: func(host *resource.Host, mockPeer *resource.Peer) {
				mockPeer.Host.ConcurrentUploadCount.Add(1)
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.995))
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
		{
			name: "freeUploadCount is empty",
			mock: func(host *resource.Host, mockPeer *resource.Peer) {
				mockPeer.Host.ConcurrentUploadCount.Add(host.ConcurrentUploadLimit.Load())
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			host := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			mockPeer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, host)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(host, mockPeer)
			tc.expect(t, e.(*evaluatorNetworkTopology).calculateFreeUploadScore(host))
		})
	}
}

func TestEvaluatorNetworkTopology_calculateHostTypeScore(t *testing.T) {
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
				peer.Host.Type = types.HostTypeSuperSeed
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
				peer.Host.Type = types.HostTypeSuperSeed
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			mockHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength))
			peer := resource.NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.mock(peer)
			tc.expect(t, e.(*evaluatorNetworkTopology).calculateHostTypeScore(peer))
		})
	}
}

func TestEvaluatorNetworkTopology_calculateIDCAffinityScore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(dstHost *resource.Host, srcHost *resource.Host)
		expect func(t *testing.T, score float64)
	}{
		{
			name: "idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.Network.IDC = ""
				srcHost.Network.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "dst host idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.Network.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "src host idc is empty",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				srcHost.Network.IDC = ""
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "idc is not the same",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.Network.IDC = "foo"
				srcHost.Network.IDC = "bar"
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
		{
			name: "idc is the same",
			mock: func(dstHost *resource.Host, srcHost *resource.Host) {
				dstHost.Network.IDC = "bal"
				srcHost.Network.IDC = "bal"
			},
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			dstHost := resource.NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			srcHost := resource.NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			tc.mock(dstHost, srcHost)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.expect(t, e.(*evaluatorNetworkTopology).calculateIDCAffinityScore(dstHost.Network.IDC, srcHost.Network.IDC))
		})
	}
}

func TestEvaluatorNetworkTopology_calculateMultiElementAffinityScore(t *testing.T) {
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
			src:  "foo|bar|baz|bac|bae|bai",
			expect: func(t *testing.T, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			tc.expect(t, e.(*evaluatorNetworkTopology).calculateMultiElementAffinityScore(tc.dst, tc.src))
		})
	}
}

func TestEvaluatorNetworkTopology_calculateNetworkTopologyScore(t *testing.T) {
	tests := []struct {
		name   string
		parent *resource.Peer
		child  *resource.Peer
		mock   func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder)
		expect func(t *testing.T, parent *resource.Peer, child *resource.Peer, score float64)
	}{
		{
			name: "calculate parent networktopology score",
			parent: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			mock: func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
				mn.Probes(parent.ID, child.ID).Return(p)
				mp.AverageRTT().Return(100*time.Millisecond, nil)
			},
			expect: func(t *testing.T, parent *resource.Peer, child *resource.Peer, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0.9))
			},
		},
		{
			name: "get average error",
			parent: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)),
			child: resource.NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig,
				resource.NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, resource.WithDigest(mockTaskDigest), resource.WithPieceLength(mockTaskPieceLength)),
				resource.NewHost(mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)),
			mock: func(parent *resource.Peer, child *resource.Peer, p networktopology.Probes, mn *networktopologymocks.MockNetworkTopologyMockRecorder, mp *networktopologymocks.MockProbesMockRecorder) {
				mn.Probes(parent.ID, child.ID).Return(p)
				mp.AverageRTT().Return(time.Duration(0), errors.New("foo"))
			},
			expect: func(t *testing.T, parent *resource.Peer, child *resource.Peer, score float64) {
				assert := assert.New(t)
				assert.Equal(score, float64(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
			e := newEvaluatorNetworkTopology(WithNetworkTopology(mockNetworkTopology))
			mockProbe := networktopologymocks.NewMockProbes(ctl)
			tc.mock(tc.parent, tc.child, mockProbe, mockNetworkTopology.EXPECT(), mockProbe.EXPECT())
			tc.expect(t, tc.parent, tc.child, e.(*evaluatorNetworkTopology).calculateNetworkTopologyScore(tc.parent.ID, tc.child.ID))
		})
	}
}
