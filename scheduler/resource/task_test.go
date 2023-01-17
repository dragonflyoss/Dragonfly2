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

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	"d7y.io/api/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
)

var (
	mockTaskURLMeta = &commonv1.UrlMeta{
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
	mockPieceInfo                   = &commonv1.PieceInfo{
		PieceNum:    1,
		RangeStart:  0,
		RangeSize:   100,
		PieceMd5:    "ad83a945518a4ef007d8b2db2ef165b3",
		PieceOffset: 10,
	}
)

func TestTask_NewTask(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "new task",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.ID, mockTaskID)
				assert.Equal(task.URL, mockTaskURL)
				assert.EqualValues(task.URLMeta, mockTaskURLMeta)
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
			tc.expect(t, NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit)))
		})
	}
}

func TestTask_LoadPeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:              "load peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:              "peer does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            idgen.PeerID("0.0.0.0"),
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
		{
			name:              "load key is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, host)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, host)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, host)
				mockPeerH := NewPeer(idgen.PeerID("127.0.0.1"), task, host)

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
			host := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

			tc.expect(t, task, host)
		})
	}
}

func TestTask_StorePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:              "store peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:              "store key is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(tc.peerID, task, mockHost)

			task.StorePeer(mockPeer)
			peer, loaded := task.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestTask_DeletePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "delete peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, loaded := task.LoadPeer(mockPeerID)
				assert.Equal(loaded, false)
			},
		},
		{
			name:              "delete key is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            "",
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
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)
			mockPeer := NewPeer(mockPeerID, task, mockHost)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

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
				mockPeerE := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerF := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)
				mockPeerG := NewPeer(idgen.PeerID("127.0.0.1"), task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta)

			tc.expect(t, mockHost, task)
		})
	}
}

func TestTask_HasAvailablePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		expect            func(t *testing.T, task *Task, mockPeer *Peer)
	}{
		{
			name:              "blocklist includes peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
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
			name:              "peer state is PeerStatePending",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				mockPeer.FSM.SetState(PeerStatePending)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name:              "peer state is PeerStateSucceeded",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				mockPeer.FSM.SetState(PeerStateSucceeded)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name:              "peer state is PeerStateRunning",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				mockPeer.FSM.SetState(PeerStateRunning)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name:              "peer state is PeerStateBackToSource",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				mockPeer.FSM.SetState(PeerStateBackToSource)
				task.StorePeer(mockPeer)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), true)
			},
		},
		{
			name:              "peer does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(task.HasAvailablePeer(set.NewSafeSet[string]()), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

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
			mockHost := NewHost(mockRawHost)
			mockSeedHost := NewHost(mockRawSeedHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)
			mockSeedPeer := NewPeer(mockSeedPeerID, task, mockSeedHost)

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
			mockHost := NewHost(mockRawHost)
			mockSeedHost := NewHost(mockRawSeedHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)
			mockSeedPeer := NewPeer(mockSeedPeerID, task, mockSeedHost)

			tc.expect(t, task, mockPeer, mockSeedPeer)
		})
	}
}

func TestTask_LoadPiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *commonv1.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, piece *commonv1.PieceInfo, loaded bool)
	}{
		{
			name:              "load piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, piece *commonv1.PieceInfo, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(piece.PieceNum, mockPieceInfo.PieceNum)
			},
		},
		{
			name:              "piece does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          2,
			expect: func(t *testing.T, piece *commonv1.PieceInfo, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
		{
			name:              "load key is zero",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          0,
			expect: func(t *testing.T, piece *commonv1.PieceInfo, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			task.StorePiece(tc.pieceInfo)
			piece, loaded := task.LoadPiece(tc.pieceNum)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestTask_StorePiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *commonv1.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, piece *commonv1.PieceInfo, loaded bool)
	}{
		{
			name:              "store piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, piece *commonv1.PieceInfo, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(piece.PieceNum, mockPieceInfo.PieceNum)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			task.StorePiece(tc.pieceInfo)
			piece, loaded := task.LoadPiece(tc.pieceNum)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestTask_DeletePiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *commonv1.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "delete piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, loaded := task.LoadPiece(mockPieceInfo.PieceNum)
				assert.Equal(loaded, false)
			},
		},
		{
			name:              "delete key does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          0,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				piece, loaded := task.LoadPiece(mockPieceInfo.PieceNum)
				assert.Equal(loaded, true)
				assert.Equal(piece.PieceNum, mockPieceInfo.PieceNum)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			task.StorePiece(tc.pieceInfo)
			task.DeletePiece(tc.pieceNum)
			tc.expect(t, task)
		})
	}
}

func TestTask_SizeScope(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		contentLength     int64
		totalPieceCount   int32
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "scope size is tiny",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     TinyFileSize,
			totalPieceCount:   1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope, err := task.SizeScope()
				assert.NoError(err)
				assert.Equal(sizeScope, commonv1.SizeScope_TINY)
			},
		},
		{
			name:              "scope size is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     0,
			totalPieceCount:   0,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope, err := task.SizeScope()
				assert.NoError(err)
				assert.Equal(sizeScope, commonv1.SizeScope_EMPTY)
			},
		},
		{
			name:              "scope size is small",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     TinyFileSize + 1,
			totalPieceCount:   1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope, err := task.SizeScope()
				assert.NoError(err)
				assert.Equal(sizeScope, commonv1.SizeScope_SMALL)
			},
		},
		{
			name:              "scope size is normal",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     TinyFileSize + 1,
			totalPieceCount:   2,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				sizeScope, err := task.SizeScope()
				assert.NoError(err)
				assert.Equal(sizeScope, commonv1.SizeScope_NORMAL)
			},
		},
		{
			name:              "invalid content length",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     -1,
			totalPieceCount:   2,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, err := task.SizeScope()
				assert.Errorf(err, "invalid content length")
			},
		},
		{
			name:              "invalid total piece count",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			contentLength:     TinyFileSize + 1,
			totalPieceCount:   -1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, err := task.SizeScope()
				assert.Errorf(err, "invalid total piece count")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			task.ContentLength.Store(tc.contentLength)
			task.TotalPieceCount.Store(tc.totalPieceCount)
			tc.expect(t, task)
		})
	}
}

func TestTask_CanBackToSource(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "task can back-to-source",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.CanBackToSource(), true)
			},
		},
		{
			name:              "task can not back-to-source",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: -1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				assert.Equal(task.CanBackToSource(), false)
			},
		},
		{
			name:              "task can back-to-source and task type is DfStore",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.Type = commonv1.TaskType_DfStore
				assert.Equal(task.CanBackToSource(), true)
			},
		},
		{
			name:              "task type is Dfcache",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.Type = commonv1.TaskType_DfCache
				assert.Equal(task.CanBackToSource(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			tc.expect(t, task)
		})
	}
}

func TestTask_CanReuseDirectPiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *commonv1.UrlMeta
		url               string
		backToSourceLimit int32
		expect            func(t *testing.T, task *Task)
	}{
		{
			name:              "task can reuse direct piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.DirectPiece = []byte{1}
				task.ContentLength.Store(1)
				assert.Equal(task.CanReuseDirectPiece(), true)
			},
		},
		{
			name:              "direct piece is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				task.ContentLength.Store(1)
				assert.Equal(task.CanReuseDirectPiece(), false)
			},
		},
		{
			name:              "content length is error",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 1,
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
			task := NewTask(tc.id, tc.url, commonv1.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			tc.expect(t, task)
		})
	}
}

func TestTask_NotifyPeers(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder)
	}{
		{
			name: "peer state is PeerStatePending",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStatePending)
				task.NotifyPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStatePending))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream is empty",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				task.NotifyPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream sending failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.NotifyPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and state changing failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.NotifyPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and notify peer successfully",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer, ms *mocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError})).Return(nil).Times(1)

				task.NotifyPeers(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := mocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)
			task.StorePeer(mockPeer)
			tc.run(t, task, mockPeer, stream, stream.EXPECT())
		})
	}
}
