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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	v1mocks "d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"
	v2mocks "d7y.io/api/v2/pkg/apis/scheduler/v2/mocks"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockPiece = &Piece{
		Number:      1,
		ParentID:    idgen.PeerIDV2(),
		Offset:      0,
		Length:      100,
		Digest:      mockPieceDigest,
		TrafficType: commonv2.TrafficType_REMOTE_PEER,
		Cost:        1 * time.Second,
		CreatedAt:   time.Now(),
	}

	mockTaskBackToSourceLimit int32 = 200
	mockTaskURL                     = "http://example.com/foo"
	mockTaskID                      = idgen.TaskIDV2(mockTaskURL, mockTaskDigest.String(), mockTaskTag, mockTaskApplication, mockTaskPieceLength, mockTaskFilters)
	mockTaskDigest                  = digest.New(digest.AlgorithmSHA256, "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4")
	mockTaskTag                     = "d7y"
	mockTaskApplication             = "foo"
	mockTaskFilters                 = []string{"bar"}
	mockTaskHeader                  = map[string]string{"content-length": "100"}
	mockTaskPieceLength       int32 = 2048
	mockConfig                      = &config.Config{
		Resource: *mockResourceConfig,
	}

	mockResourceConfig = &config.ResourceConfig{
		Task: config.TaskConfig{
			DownloadTiny: config.DownloadTinyConfig{
				Scheme:  config.DefaultResourceTaskDownloadTinyScheme,
				Timeout: config.DefaultResourceTaskDownloadTinyTimeout,
				TLS: config.DownloadTinyTLSClientConfig{
					InsecureSkipVerify: true,
				},
			},
		},
	}
	mockPieceDigest = digest.New(digest.AlgorithmMD5, "ad83a945518a4ef007d8b2db2ef165b3")
)

func TestTask_NewTask(t *testing.T) {
	tests := []struct {
		name    string
		options []TaskOption
		expect  func(t *testing.T, task *Task)
	}{
		{
			name:    "new task",
			options: []TaskOption{},
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.ID, mockTaskID)
				assert.Equal(task.Type, commonv2.TaskType_DFDAEMON)
				assert.Equal(task.URL, mockTaskURL)
				assert.Nil(task.Digest)
				assert.Equal(task.Tag, mockTaskTag)
				assert.Equal(task.Application, mockTaskApplication)
				assert.EqualValues(task.Filters, mockTaskFilters)
				assert.EqualValues(task.Header, mockTaskHeader)
				assert.Equal(task.PieceLength, int32(0))
				assert.Empty(task.DirectPiece)
				assert.Equal(task.ContentLength.Load(), int64(-1))
				assert.Equal(task.TotalPieceCount.Load(), int32(0))
				assert.Equal(task.BackToSourceLimit.Load(), int32(200))
				assert.Equal(task.BackToSourcePeers.Len(), uint(0))
				assert.Equal(task.FSM.Current(), TaskStatePending)
				assert.Empty(task.Pieces)
				assert.Equal(task.PeerCount(), 0)
				assert.NotEqual(task.CreatedAt.Load(), 0)
				assert.NotEqual(task.UpdatedAt.Load(), 0)
				assert.NotNil(task.Log)
			},
		},
		{
			name:    "new task with piece length",
			options: []TaskOption{WithPieceLength(mockTaskPieceLength)},
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.ID, mockTaskID)
				assert.Equal(task.Type, commonv2.TaskType_DFDAEMON)
				assert.Equal(task.URL, mockTaskURL)
				assert.Nil(task.Digest)
				assert.Equal(task.Tag, mockTaskTag)
				assert.Equal(task.Application, mockTaskApplication)
				assert.EqualValues(task.Filters, mockTaskFilters)
				assert.EqualValues(task.Header, mockTaskHeader)
				assert.Equal(task.PieceLength, mockTaskPieceLength)
				assert.Empty(task.DirectPiece)
				assert.Equal(task.ContentLength.Load(), int64(-1))
				assert.Equal(task.TotalPieceCount.Load(), int32(0))
				assert.Equal(task.BackToSourceLimit.Load(), int32(200))
				assert.Equal(task.BackToSourcePeers.Len(), uint(0))
				assert.Equal(task.FSM.Current(), TaskStatePending)
				assert.Empty(task.Pieces)
				assert.Equal(task.PeerCount(), 0)
				assert.NotEqual(task.CreatedAt.Load(), 0)
				assert.NotEqual(task.UpdatedAt.Load(), 0)
				assert.NotNil(task.Log)
			},
		},
		{
			name:    "new task with digest",
			options: []TaskOption{WithDigest(mockTaskDigest)},
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.ID, mockTaskID)
				assert.Equal(task.Type, commonv2.TaskType_DFDAEMON)
				assert.Equal(task.URL, mockTaskURL)
				assert.EqualValues(task.Digest, mockTaskDigest)
				assert.Equal(task.Tag, mockTaskTag)
				assert.Equal(task.Application, mockTaskApplication)
				assert.EqualValues(task.Filters, mockTaskFilters)
				assert.EqualValues(task.Header, mockTaskHeader)
				assert.Equal(task.PieceLength, int32(0))
				assert.Empty(task.DirectPiece)
				assert.Equal(task.ContentLength.Load(), int64(-1))
				assert.Equal(task.TotalPieceCount.Load(), int32(0))
				assert.Equal(task.BackToSourceLimit.Load(), int32(200))
				assert.Equal(task.BackToSourcePeers.Len(), uint(0))
				assert.Equal(task.FSM.Current(), TaskStatePending)
				assert.Empty(task.Pieces)
				assert.Equal(task.PeerCount(), 0)
				assert.NotEqual(task.CreatedAt.Load(), 0)
				assert.NotEqual(task.UpdatedAt.Load(), 0)
				assert.NotNil(task.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit, tc.options...))
		})
	}
}

func TestTask_LoadPeer(t *testing.T) {
	tests := []struct {
		name   string
		peerID string
		expect func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:   "load peer",
			peerID: mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:   "peer does not exist",
			peerID: idgen.PeerIDV2(),
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
		{
			name:   "load key is empty",
			peerID: "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)

			task.StorePeer(mockPeer)
			peer, loaded := task.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestTask_LoadRandomPeers(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, task *Task, host *Host)
	}{
		{
			name: "load random peers",
			expect: func(t *testing.T, task *Task, host *Host) {
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, host)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, host)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, host)
				mockPeerH := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, host)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				task.StorePeer(mockPeerH)

				assert := assert.New(t)
				peers := task.LoadRandomPeers(0)
				assert.Equal(len(peers), 0)

				peers = task.LoadRandomPeers(1)
				assert.Equal(len(peers), 1)

				peers = task.LoadRandomPeers(2)
				assert.Equal(len(peers), 2)

				peers = task.LoadRandomPeers(3)
				assert.Equal(len(peers), 3)

				peers = task.LoadRandomPeers(4)
				assert.Equal(len(peers), 4)

				peers = task.LoadRandomPeers(5)
				assert.Equal(len(peers), 4)
			},
		},
		{
			name: "load empty peers",
			expect: func(t *testing.T, task *Task, host *Host) {
				assert := assert.New(t)
				peers := task.LoadRandomPeers(0)
				assert.Equal(len(peers), 0)

				peers = task.LoadRandomPeers(1)
				assert.Equal(len(peers), 0)

				peers = task.LoadRandomPeers(2)
				assert.Equal(len(peers), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, task, host)
		})
	}
}

func TestTask_StorePeer(t *testing.T) {
	tests := []struct {
		name   string
		peerID string
		expect func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:   "store peer",
			peerID: mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:   "store key is empty",
			peerID: "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(tc.peerID, mockResourceConfig, task, mockHost)

			task.StorePeer(mockPeer)
			peer, loaded := task.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestTask_DeletePeer(t *testing.T) {
	tests := []struct {
		name   string
		peerID string
		expect func(t *testing.T, task *Task)
	}{
		{
			name:   "delete peer",
			peerID: mockPeerID,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, loaded := task.LoadPeer(mockPeerID)
				assert.Equal(loaded, false)
			},
		},
		{
			name:   "delete key is empty",
			peerID: "",
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				peer, loaded := task.LoadPeer(mockPeerID)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)

			task.StorePeer(mockPeer)
			task.DeletePeer(tc.peerID)
			tc.expect(t, task)
		})
	}
}

func TestTask_PeerCount(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockPeer *Peer, task *Task)
	}{
		{
			name: "task has no peers",
			expect: func(t *testing.T, mockPeer *Peer, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.PeerCount(), 0)
			},
		},
		{
			name: "task has peers",
			expect: func(t *testing.T, mockPeer *Peer, task *Task) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				assert.Equal(task.PeerCount(), 1)
				task.DeletePeer(mockPeer.ID)
				assert.Equal(task.PeerCount(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)

			tc.expect(t, mockPeer, task)
		})
	}
}

func TestTask_AddPeerEdge(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "add peer edge failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerF, mockPeerG)
				assert.NoError(err)
				assert.Equal(mockPeerF.Children()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerE)
				assert.Error(err)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))
			},
		},
		{
			name: "add peer edge",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerE, mockPeerG)
				assert.NoError(err)
				assert.Equal(len(mockPeerE.Children()), 2)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerF)
				assert.NoError(err)
				assert.Equal(len(mockPeerF.Parents()), 2)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(3))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_DeletePeerInEdges(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "delete peer inedges failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				err := task.DeletePeerInEdges(mockPeerID)
				assert.Error(err)
			},
		},
		{
			name: "delete peer inedges",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				var (
					err    error
					degree int
				)
				err = task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerE, mockPeerG)
				assert.NoError(err)
				assert.Equal(len(mockPeerE.Children()), 2)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerF)
				assert.NoError(err)
				assert.Equal(len(mockPeerF.Parents()), 2)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(3))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerInEdges(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(len(mockPeerE.Children()), 2)
				assert.Equal(len(mockPeerF.Parents()), 2)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(3))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerInEdges(mockPeerF.ID)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerInEdges(mockPeerG.ID)
				assert.NoError(err)
				degree, err = task.PeerDegree(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				degree, err = task.PeerDegree(mockPeerF.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				degree, err = task.PeerDegree(mockPeerG.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_DeletePeerOutEdges(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "delete peer outedges failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				err := task.DeletePeerOutEdges(mockPeerID)
				assert.Error(err)
			},
		},
		{
			name: "delete peer outedges",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				var (
					err    error
					degree int
				)
				err = task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerE, mockPeerG)
				assert.NoError(err)
				assert.Equal(len(mockPeerE.Children()), 2)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerF)
				assert.NoError(err)
				assert.Equal(len(mockPeerF.Parents()), 2)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(3))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerOutEdges(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(len(mockPeerF.Parents()), 1)
				assert.Equal(len(mockPeerG.Parents()), 0)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerOutEdges(mockPeerF.ID)
				assert.NoError(err)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.DeletePeerOutEdges(mockPeerG.ID)
				assert.NoError(err)
				degree, err = task.PeerDegree(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				degree, err = task.PeerDegree(mockPeerF.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				degree, err = task.PeerDegree(mockPeerG.ID)
				assert.NoError(err)
				assert.Equal(degree, 0)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(mockHost.UploadCount.Load(), int64(3))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_CanAddPeerEdge(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "peer can not add edge",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerF, mockPeerG)
				assert.NoError(err)
				assert.Equal(mockPeerF.Children()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerF.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				assert.False(task.CanAddPeerEdge(mockPeerG.ID, mockPeerE.ID))
			},
		},
		{
			name: "peer can add edge",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerE, mockPeerG)
				assert.NoError(err)
				assert.Equal(len(mockPeerE.Children()), 2)
				assert.Equal(mockPeerG.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				assert.True(task.CanAddPeerEdge(mockPeerG.ID, mockPeerF.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_PeerDegree(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "get peer degree failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				_, err := task.PeerDegree(mockPeerID)
				assert.Error(err)
			},
		},
		{
			name: "peer get degree",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerE)
				assert.NoError(err)
				assert.Equal(mockPeerE.Parents()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				degree, err := task.PeerDegree(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(degree, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_PeerInDegree(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "get peer indegree failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				_, err := task.PeerInDegree(mockPeerID)
				assert.Error(err)
			},
		},
		{
			name: "peer get indegree",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerE)
				assert.NoError(err)
				assert.Equal(mockPeerE.Parents()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				degree, err := task.PeerInDegree(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(degree, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_PeerOutDegree(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, mockHost *Host, task *Task)
	}{
		{
			name: "get peer outdegree failed",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				_, err := task.PeerOutDegree(mockPeerID)
				assert.Error(err)
			},
		},
		{
			name: "peer get outdegree",
			expect: func(t *testing.T, mockHost *Host, task *Task) {
				assert := assert.New(t)
				mockPeerE := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerF := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)
				mockPeerG := NewPeer(idgen.PeerIDV1("127.0.0.1"), mockResourceConfig, task, mockHost)

				task.StorePeer(mockPeerE)
				task.StorePeer(mockPeerF)
				task.StorePeer(mockPeerG)
				mockHost.StorePeer(mockPeerE)
				mockHost.StorePeer(mockPeerF)
				mockHost.StorePeer(mockPeerG)

				err := task.AddPeerEdge(mockPeerE, mockPeerF)
				assert.NoError(err)
				assert.Equal(mockPeerE.Children()[0].ID, mockPeerF.ID)
				assert.Equal(mockPeerF.Parents()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(1))
				assert.Equal(mockHost.UploadCount.Load(), int64(1))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				err = task.AddPeerEdge(mockPeerG, mockPeerE)
				assert.NoError(err)
				assert.Equal(mockPeerE.Parents()[0].ID, mockPeerG.ID)
				assert.Equal(mockPeerG.Children()[0].ID, mockPeerE.ID)
				assert.Equal(mockHost.ConcurrentUploadCount.Load(), int32(2))
				assert.Equal(mockHost.UploadCount.Load(), int64(2))
				assert.Equal(mockHost.PeerCount.Load(), int32(3))

				degree, err := task.PeerOutDegree(mockPeerE.ID)
				assert.NoError(err)
				assert.Equal(degree, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_HasAvailablePeer(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, task *Task, mockPeer *Peer)
	}{
		{
			name: "blocklist includes peer",
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.FSM.SetState(PeerStatePending)
				task.StorePeer(mockPeer)

				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockPeer.ID)
				assert.Equal(task.HasAvailablePeer(blocklist), false)
			},
		},
		{
			name: "peer state is PeerStateSucceeded",
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerIDV2()
				mockPeer.FSM.SetState(PeerStateSucceeded)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name: "peer state is PeerStateRunning",
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerIDV2()
				mockPeer.FSM.SetState(PeerStateRunning)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name: "peer state is PeerStateBackToSource",
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerIDV2()
				mockPeer.FSM.SetState(PeerStateBackToSource)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name: "peer does not exist",
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)

			tc.expect(t, task, mockPeer)
		})
	}
}

func TestTask_LoadSeedPeer(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer)
	}{
		{
			name: "load seed peer",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)
				peer, loaded := task.LoadSeedPeer()
				assert.True(loaded)
				assert.Equal(peer.ID, mockSeedPeer.ID)
			},
		},
		{
			name: "load latest seed peer",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				mockPeer.Host.Type = types.HostTypeSuperSeed
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)

				mockPeer.UpdatedAt.Store(time.Now())
				mockSeedPeer.UpdatedAt.Store(time.Now().Add(1 * time.Second))

				peer, loaded := task.LoadSeedPeer()
				assert.True(loaded)
				assert.Equal(peer.ID, mockSeedPeer.ID)
			},
		},
		{
			name: "peers is empty",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				_, loaded := task.LoadSeedPeer()
				assert.False(loaded)
			},
		},
		{
			name: "seed peers is empty",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				_, loaded := task.LoadSeedPeer()
				assert.False(loaded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockSeedHost := NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)
			mockSeedPeer := NewPeer(mockSeedPeerID, mockResourceConfig, task, mockSeedHost)

			tc.expect(t, task, mockPeer, mockSeedPeer)
		})
	}
}

func TestTask_IsSeedPeerFailed(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer)
	}{
		{
			name: "seed peer state is PeerStateFailed",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)
				mockSeedPeer.FSM.SetState(PeerStateFailed)

				assert.True(task.IsSeedPeerFailed())
			},
		},
		{
			name: "can not find seed peer",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)

				assert.False(task.IsSeedPeerFailed())
			},
		},
		{
			name: "seed peer state is PeerStateSucceeded",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)
				mockSeedPeer.FSM.SetState(PeerStateSucceeded)

				assert.False(task.IsSeedPeerFailed())
			},
		},
		{
			name: "seed peer failed timeout",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)
				mockSeedPeer.CreatedAt.Store(time.Now().Add(-SeedPeerFailedTimeout))
				mockSeedPeer.FSM.SetState(PeerStateFailed)

				assert.False(task.IsSeedPeerFailed())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockSeedHost := NewHost(
				mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
				mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)
			mockSeedPeer := NewPeer(mockSeedPeerID, mockResourceConfig, task, mockSeedHost)

			tc.expect(t, task, mockPeer, mockSeedPeer)
		})
	}
}

func TestTask_LoadPiece(t *testing.T) {
	tests := []struct {
		name        string
		piece       *Piece
		pieceNumber int32
		expect      func(t *testing.T, piece *Piece, loaded bool)
	}{
		{
			name:        "load piece",
			piece:       mockPiece,
			pieceNumber: mockPiece.Number,
			expect: func(t *testing.T, piece *Piece, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.EqualValues(piece.Digest, mockPiece.Digest)
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.Equal(piece.CreatedAt, mockPiece.CreatedAt)
			},
		},
		{
			name:        "piece does not exist",
			piece:       mockPiece,
			pieceNumber: 2,
			expect: func(t *testing.T, piece *Piece, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
		{
			name:        "load key is zero",
			piece:       mockPiece,
			pieceNumber: 0,
			expect: func(t *testing.T, piece *Piece, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			task.StorePiece(tc.piece)
			piece, loaded := task.LoadPiece(tc.pieceNumber)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestTask_StorePiece(t *testing.T) {
	tests := []struct {
		name        string
		piece       *Piece
		pieceNumber int32
		expect      func(t *testing.T, piece *Piece, loaded bool)
	}{
		{
			name:        "store piece",
			piece:       mockPiece,
			pieceNumber: mockPiece.Number,
			expect: func(t *testing.T, piece *Piece, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(piece.Number, mockPiece.Number)
				assert.Equal(piece.ParentID, mockPiece.ParentID)
				assert.Equal(piece.Offset, mockPiece.Offset)
				assert.Equal(piece.Length, mockPiece.Length)
				assert.EqualValues(piece.Digest, mockPiece.Digest)
				assert.Equal(piece.TrafficType, mockPiece.TrafficType)
				assert.Equal(piece.Cost, mockPiece.Cost)
				assert.Equal(piece.CreatedAt, mockPiece.CreatedAt)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			task.StorePiece(tc.piece)
			piece, loaded := task.LoadPiece(tc.pieceNumber)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestTask_DeletePiece(t *testing.T) {
	tests := []struct {
		name        string
		piece       *Piece
		pieceNumber int32
		expect      func(t *testing.T, task *Task)
	}{
		{
			name:        "delete piece",
			piece:       mockPiece,
			pieceNumber: mockPiece.Number,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, loaded := task.LoadPiece(mockPiece.Number)
				assert.Equal(loaded, false)
			},
		},
		{
			name:        "delete key does not exist",
			piece:       mockPiece,
			pieceNumber: 0,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				piece, loaded := task.LoadPiece(mockPiece.Number)
				assert.Equal(loaded, true)
				assert.Equal(piece.Number, mockPiece.Number)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)

			task.StorePiece(tc.piece)
			task.DeletePiece(tc.pieceNumber)
			tc.expect(t, task)
		})
	}
}

func TestTask_SizeScope(t *testing.T) {
	tests := []struct {
		name            string
		contentLength   int64
		totalPieceCount int32
		expect          func(t *testing.T, task *Task)
	}{
		{
			name:            "scope size is tiny",
			contentLength:   TinyFileSize,
			totalPieceCount: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_TINY)
			},
		},
		{
			name:            "scope size is empty",
			contentLength:   0,
			totalPieceCount: 0,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_EMPTY)
			},
		},
		{
			name:            "scope size is small",
			contentLength:   TinyFileSize + 1,
			totalPieceCount: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_SMALL)
			},
		},
		{
			name:            "scope size is normal",
			contentLength:   TinyFileSize + 1,
			totalPieceCount: 2,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_NORMAL)
			},
		},
		{
			name:            "invalid content length",
			contentLength:   -1,
			totalPieceCount: 2,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_UNKNOW)
			},
		},
		{
			name:            "invalid total piece count",
			contentLength:   TinyFileSize + 1,
			totalPieceCount: -1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope := task.SizeScope()
				assert.Equal(sizeScope, commonv2.SizeScope_UNKNOW)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			task.ContentLength.Store(tc.contentLength)
			task.TotalPieceCount.Store(tc.totalPieceCount)
			tc.expect(t, task)
		})
	}
}

func TestTask_CanBackToSource(t *testing.T) {
	tests := []struct {
		name              string
		backToSourceLimit int32
		run               func(t *testing.T, task *Task)
	}{
		{
			name:              "task can back-to-source",
			backToSourceLimit: 1,
			run: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.CanBackToSource(), true)
			},
		},
		{
			name:              "task can not back-to-source",
			backToSourceLimit: -1,
			run: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.CanBackToSource(), false)
			},
		},
		{
			name:              "task can back-to-source and task type is DFSTORE",
			backToSourceLimit: 1,
			run: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.Type = commonv2.TaskType_DFSTORE
				assert.Equal(task.CanBackToSource(), true)
			},
		},
		{
			name:              "task type is DFCACHE",
			backToSourceLimit: 1,
			run: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.Type = commonv2.TaskType_DFCACHE
				assert.Equal(task.CanBackToSource(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, tc.backToSourceLimit)
			tc.run(t, task)
		})
	}
}

func TestTask_CanReuseDirectPiece(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, task *Task)
	}{
		{
			name: "task can reuse direct piece",
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.DirectPiece = []byte{1}
				task.ContentLength.Store(1)
				assert.Equal(task.CanReuseDirectPiece(), true)
			},
		},
		{
			name: "direct piece is empty",
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.ContentLength.Store(1)
				assert.Equal(task.CanReuseDirectPiece(), false)
			},
		},
		{
			name: "content length is error",
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.DirectPiece = []byte{1}
				task.ContentLength.Store(2)
				assert.Equal(task.CanReuseDirectPiece(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			tc.expect(t, task)
		})
	}
}

func TestTask_ReportPieceResultToPeers(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder)
	}{
		{
			name: "peer state is PeerStatePending",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStatePending)
				task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStatePending))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream is empty",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream sending failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreReportPieceResultStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and state changing failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreReportPieceResultStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and report peer successfully",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *v1mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreReportPieceResultStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(nil).Times(1)

				task.ReportPieceResultToPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := v1mocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)
			task.StorePeer(mockPeer)
			tc.run(t, task, mockPeer, stream, stream.EXPECT())
		})
	}
}

func TestTask_AnnouncePeers(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder)
	}{
		{
			name: "peer state is PeerStatePending",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStatePending)
				task.AnnouncePeers(&schedulerv2.AnnouncePeerResponse{}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStatePending))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream is empty",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				task.AnnouncePeers(&schedulerv2.AnnouncePeerResponse{}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream sending failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreAnnouncePeerStream(stream)
				ms.Send(gomock.Eq(&schedulerv2.AnnouncePeerResponse{})).Return(errors.New("foo")).Times(1)

				task.AnnouncePeers(&schedulerv2.AnnouncePeerResponse{}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and state changing failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreAnnouncePeerStream(stream)
				ms.Send(gomock.Eq(&schedulerv2.AnnouncePeerResponse{})).Return(errors.New("foo")).Times(1)

				task.AnnouncePeers(&schedulerv2.AnnouncePeerResponse{}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and announce peer successfully",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer, ms *v2mocks.MockScheduler_AnnouncePeerServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreAnnouncePeerStream(stream)
				ms.Send(gomock.Eq(&schedulerv2.AnnouncePeerResponse{})).Return(nil).Times(1)

				task.AnnouncePeers(&schedulerv2.AnnouncePeerResponse{}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := v2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			task := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, task, mockHost)
			task.StorePeer(mockPeer)
			tc.run(t, task, mockPeer, stream, stream.EXPECT())
		})
	}
}
