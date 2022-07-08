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

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	rpcschedulermocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/mocks"
)

var (
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
	mockPieceInfo                   = &base.PieceInfo{
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
		urlMeta           *base.UrlMeta
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
				assert.Equal(task.ContentLength.Load(), int64(0))
				assert.Equal(task.TotalPieceCount.Load(), int32(0))
				assert.Equal(task.BackToSourceLimit.Load(), int32(200))
				assert.Equal(task.BackToSourcePeers.Len(), uint(0))
				assert.Equal(task.FSM.Current(), TaskStatePending)
				assert.Empty(task.Pieces)
				assert.Empty(task.Peers)
				assert.NotEqual(task.CreateAt.Load(), 0)
				assert.NotEqual(task.UpdateAt.Load(), 0)
				assert.NotNil(task.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit)))
		})
	}
}

func TestTask_LoadPeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, peer *Peer, ok bool)
	}{
		{
			name:              "load peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
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
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
		{
			name:              "load key is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            "",
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

			task.StorePeer(mockPeer)
			peer, ok := task.LoadPeer(tc.peerID)
			tc.expect(t, peer, ok)
		})
	}
}

func TestTask_StorePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, peer *Peer, ok bool)
	}{
		{
			name:              "store peer",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
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
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(tc.peerID, task, mockHost)

			task.StorePeer(mockPeer)
			peer, ok := task.LoadPeer(tc.peerID)
			tc.expect(t, peer, ok)
		})
	}
}

func TestTask_LoadOrStorePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		peerID            string
		expect            func(t *testing.T, task *Task, mockPeer *Peer)
	}{
		{
			name:              "load peer exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				peer, ok := task.LoadOrStorePeer(mockPeer)

				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:              "load peer does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			peerID:            mockPeerID,
			expect: func(t *testing.T, task *Task, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				peer, ok := task.LoadOrStorePeer(mockPeer)

				assert.Equal(ok, false)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

			task.StorePeer(mockPeer)
			tc.expect(t, task, mockPeer)
		})
	}
}

func TestTask_DeletePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
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
				_, ok := task.LoadPeer(mockPeerID)
				assert.Equal(ok, false)
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
				peer, ok := task.LoadPeer(mockPeerID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)

			task.StorePeer(mockPeer)
			task.DeletePeer(tc.peerID)
			tc.expect(t, task)
		})
	}
}

func TestTask_HasAvailablePeer(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		expect            func(t *testing.T, task *Task, mockPeer *Peer)
	}{
		{
			name:              "len available peers",
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
				assert.Equal(task.HasAvailablePeer(), true)
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
				assert.Equal(task.HasAvailablePeer(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
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
				peer, ok := task.LoadSeedPeer()
				assert.True(ok)
				assert.Equal(peer.ID, mockSeedPeer.ID)
			},
		},
		{
			name: "load latest seed peer",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				mockPeer.Host.Type = HostTypeSuperSeed
				task.StorePeer(mockPeer)
				task.StorePeer(mockSeedPeer)

				mockPeer.UpdateAt.Store(time.Now())
				mockSeedPeer.UpdateAt.Store(time.Now().Add(1 * time.Second))

				peer, ok := task.LoadSeedPeer()
				assert.True(ok)
				assert.Equal(peer.ID, mockSeedPeer.ID)
			},
		},
		{
			name: "peers is empty",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				_, ok := task.LoadSeedPeer()
				assert.False(ok)
			},
		},
		{
			name: "seed peers is empty",
			expect: func(t *testing.T, task *Task, mockPeer *Peer, mockSeedPeer *Peer) {
				assert := assert.New(t)
				task.StorePeer(mockPeer)
				_, ok := task.LoadSeedPeer()
				assert.False(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockSeedHost := NewHost(mockRawSeedHost, WithHostType(HostTypeSuperSeed))
			task := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
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
				mockSeedPeer.CreateAt.Store(time.Now().Add(-SeedPeerFailedTimeout))
				mockSeedPeer.FSM.SetState(PeerStateFailed)

				assert.False(task.IsSeedPeerFailed())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			mockSeedHost := NewHost(mockRawSeedHost, WithHostType(HostTypeSuperSeed))
			task := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
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
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *base.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, piece *base.PieceInfo, ok bool)
	}{
		{
			name:              "load piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, piece *base.PieceInfo, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
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
			expect: func(t *testing.T, piece *base.PieceInfo, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
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
			expect: func(t *testing.T, piece *base.PieceInfo, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			task.StorePiece(tc.pieceInfo)
			piece, ok := task.LoadPiece(tc.pieceNum)
			tc.expect(t, piece, ok)
		})
	}
}

func TestTask_StorePiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *base.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, piece *base.PieceInfo, ok bool)
	}{
		{
			name:              "store piece",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, piece *base.PieceInfo, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(piece.PieceNum, mockPieceInfo.PieceNum)
			},
		},
		{
			name:              "store key is empty",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          0,
			expect: func(t *testing.T, piece *base.PieceInfo, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(piece.PieceNum, int32(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			tc.pieceInfo.PieceNum = tc.pieceNum
			task.StorePiece(tc.pieceInfo)
			piece, ok := task.LoadPiece(tc.pieceNum)
			tc.expect(t, piece, ok)
		})
	}
}

func TestTask_LoadOrStorePiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *base.PieceInfo
		pieceNum          int32
		expect            func(t *testing.T, task *Task, mockPiece *base.PieceInfo)
	}{
		{
			name:              "load piece exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, task *Task, mockPiece *base.PieceInfo) {
				assert := assert.New(t)
				peer, ok := task.LoadOrStorePiece(mockPiece)

				assert.Equal(ok, true)
				assert.Equal(peer.PieceNum, mockPiece.PieceNum)
			},
		},
		{
			name:              "load piece does not exist",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: mockTaskBackToSourceLimit,
			pieceInfo:         mockPieceInfo,
			pieceNum:          mockPieceInfo.PieceNum,
			expect: func(t *testing.T, task *Task, mockPiece *base.PieceInfo) {
				assert := assert.New(t)
				mockPiece.PieceNum = 2
				peer, ok := task.LoadOrStorePiece(mockPiece)

				assert.Equal(ok, false)
				assert.Equal(peer.PieceNum, mockPiece.PieceNum)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

			task.StorePiece(tc.pieceInfo)
			tc.expect(t, task, tc.pieceInfo)
		})
	}
}

func TestTask_DeletePiece(t *testing.T) {
	tests := []struct {
		name              string
		id                string
		urlMeta           *base.UrlMeta
		url               string
		backToSourceLimit int32
		pieceInfo         *base.PieceInfo
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
				_, ok := task.LoadPiece(mockPieceInfo.PieceNum)
				assert.Equal(ok, false)
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
				piece, ok := task.LoadPiece(mockPieceInfo.PieceNum)
				assert.Equal(ok, true)
				assert.Equal(piece.PieceNum, mockPieceInfo.PieceNum)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))

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
		urlMeta           *base.UrlMeta
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
				assert.Equal(sizeScope, base.SizeScope_TINY)
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
				assert.Equal(sizeScope, base.SizeScope_SMALL)
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
				assert.Equal(sizeScope, base.SizeScope_NORMAL)
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
			totalPieceCount:   0,
			expect: func(t *testing.T, task *Task) {
				assert := assert.New(t)
				_, err := task.SizeScope()
				assert.Errorf(err, "invalid total piece count")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
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
		urlMeta           *base.UrlMeta
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
			name:              "task can not base-to-source",
			id:                mockTaskID,
			urlMeta:           mockTaskURLMeta,
			url:               mockTaskURL,
			backToSourceLimit: 0,
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
				task.Type = base.TaskType_DfStore
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
				task.Type = base.TaskType_DfCache
				assert.Equal(task.CanBackToSource(), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := NewTask(tc.id, tc.url, base.TaskType_Normal, tc.urlMeta, WithBackToSourceLimit(tc.backToSourceLimit))
			tc.expect(t, task)
		})
	}
}

func TestTask_NotifyPeers(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder)
	}{
		{
			name: "peer state is PeerStatePending",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStatePending)
				task.NotifyPeers(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStatePending))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream is empty",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				task.NotifyPeers(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and stream sending failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.NotifyPeers(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and state changing failed",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(errors.New("foo")).Times(1)

				task.NotifyPeers(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateRunning))
			},
		},
		{
			name: "peer state is PeerStateRunning and notify peer successfully",
			run: func(t *testing.T, task *Task, mockPeer *Peer, stream rpcscheduler.Scheduler_ReportPieceResultServer, ms *rpcschedulermocks.MockScheduler_ReportPieceResultServerMockRecorder) {
				mockPeer.FSM.SetState(PeerStateRunning)
				mockPeer.StoreStream(stream)
				ms.Send(gomock.Eq(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError})).Return(nil).Times(1)

				task.NotifyPeers(&rpcscheduler.PeerPacket{Code: base.Code_SchedTaskStatusError}, PeerEventDownloadFailed)

				assert := assert.New(t)
				assert.True(mockPeer.FSM.Is(PeerStateFailed))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			stream := rpcschedulermocks.NewMockScheduler_ReportPieceResultServer(ctl)

			mockHost := NewHost(mockRawHost)
			task := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, task, mockHost)
			task.StorePeer(mockPeer)
			tc.run(t, task, mockPeer, stream, stream.EXPECT())
		})
	}
}
