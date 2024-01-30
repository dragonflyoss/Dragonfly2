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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"
	v1mocks "d7y.io/api/v2/pkg/apis/scheduler/v1/mocks"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"
	v2mocks "d7y.io/api/v2/pkg/apis/scheduler/v2/mocks"

	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
)

var (
	mockPeerID     = idgen.PeerIDV1("127.0.0.1")
	mockSeedPeerID = idgen.SeedPeerIDV1("127.0.0.1")
)

func TestPeer_NewPeer(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	stream := v2mocks.NewMockScheduler_AnnouncePeerServer(ctl)

	tests := []struct {
		name    string
		id      string
		options []PeerOption
		expect  func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host)
	}{
		{
			name:    "new peer",
			id:      mockPeerID,
			options: []PeerOption{},
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Nil(peer.Range)
				assert.Equal(peer.Priority, commonv2.Priority_LEVEL0)
				assert.Empty(peer.Pieces)
				assert.Empty(peer.FinishedPieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.ReportPieceResultStream)
				assert.Empty(peer.AnnouncePeerStream)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Equal(peer.BlockParents.Len(), uint(0))
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.CreatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
		{
			name:    "new peer with priority",
			id:      mockPeerID,
			options: []PeerOption{WithPriority(commonv2.Priority_LEVEL4)},
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Nil(peer.Range)
				assert.Equal(peer.Priority, commonv2.Priority_LEVEL4)
				assert.Empty(peer.Pieces)
				assert.Empty(peer.FinishedPieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.ReportPieceResultStream)
				assert.Empty(peer.AnnouncePeerStream)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Equal(peer.BlockParents.Len(), uint(0))
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.CreatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
		{
			name: "new peer with range",
			id:   mockPeerID,
			options: []PeerOption{WithRange(nethttp.Range{
				Start:  1,
				Length: 10,
			})},
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.EqualValues(peer.Range, &nethttp.Range{Start: 1, Length: 10})
				assert.Equal(peer.Priority, commonv2.Priority_LEVEL0)
				assert.Empty(peer.Pieces)
				assert.Empty(peer.FinishedPieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.ReportPieceResultStream)
				assert.Empty(peer.AnnouncePeerStream)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Equal(peer.BlockParents.Len(), uint(0))
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.CreatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
		{
			name:    "new peer with AnnouncePeerStream",
			id:      mockPeerID,
			options: []PeerOption{WithAnnouncePeerStream(stream)},
			expect: func(t *testing.T, peer *Peer, mockTask *Task, mockHost *Host) {
				assert := assert.New(t)
				assert.Equal(peer.ID, mockPeerID)
				assert.Nil(peer.Range)
				assert.Equal(peer.Priority, commonv2.Priority_LEVEL0)
				assert.Empty(peer.Pieces)
				assert.Empty(peer.FinishedPieces)
				assert.Equal(len(peer.PieceCosts()), 0)
				assert.Empty(peer.ReportPieceResultStream)
				assert.NotEmpty(peer.AnnouncePeerStream)
				assert.Equal(peer.FSM.Current(), PeerStatePending)
				assert.EqualValues(peer.Task, mockTask)
				assert.EqualValues(peer.Host, mockHost)
				assert.Equal(peer.BlockParents.Len(), uint(0))
				assert.Equal(peer.NeedBackToSource.Load(), false)
				assert.NotEqual(peer.PieceUpdatedAt.Load(), 0)
				assert.NotEqual(peer.CreatedAt.Load(), 0)
				assert.NotEqual(peer.UpdatedAt.Load(), 0)
				assert.NotNil(peer.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			tc.expect(t, NewPeer(tc.id, mockResourceConfig, mockTask, mockHost, tc.options...), mockTask, mockHost)
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
				peer.AppendPieceCost(time.Duration(1))
				costs := peer.PieceCosts()
				assert.Equal(costs[0], time.Duration(1))
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
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

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
				peer.AppendPieceCost(time.Duration(1))
				costs := peer.PieceCosts()
				assert.Equal(costs[0], time.Duration(1))
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
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

			tc.expect(t, peer)
		})
	}
}

func TestPeer_LoadReportPieceResultStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "load stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreReportPieceResultStream(stream)
				newStream, loaded := peer.LoadReportPieceResultStream()
				assert.Equal(loaded, true)
				assert.EqualValues(newStream, stream)
			},
		},
		{
			name: "stream does not exist",
			expect: func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				_, loaded := peer.LoadReportPieceResultStream()
				assert.Equal(loaded, false)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_StoreReportPieceResultStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "store stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreReportPieceResultStream(stream)
				newStream, loaded := peer.LoadReportPieceResultStream()
				assert.Equal(loaded, true)
				assert.EqualValues(newStream, stream)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_DeleteReportPieceResultStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "delete stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.StoreReportPieceResultStream(stream)
				peer.DeleteReportPieceResultStream()
				_, loaded := peer.LoadReportPieceResultStream()
				assert.Equal(loaded, false)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_LoadAnnouncePeerStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer)
	}{
		{
			name: "load stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer) {
				assert := assert.New(t)
				peer.StoreAnnouncePeerStream(stream)
				newStream, loaded := peer.LoadAnnouncePeerStream()
				assert.Equal(loaded, true)
				assert.EqualValues(newStream, stream)
			},
		},
		{
			name: "stream does not exist",
			expect: func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer) {
				assert := assert.New(t)
				_, loaded := peer.LoadAnnouncePeerStream()
				assert.Equal(loaded, false)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_StoreAnnouncePeerStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer)
	}{
		{
			name: "store stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer) {
				assert := assert.New(t)
				peer.StoreAnnouncePeerStream(stream)
				newStream, loaded := peer.LoadAnnouncePeerStream()
				assert.Equal(loaded, true)
				assert.EqualValues(newStream, stream)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_DeleteAnnouncePeerStream(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer)
	}{
		{
			name: "delete stream",
			expect: func(t *testing.T, peer *Peer, stream schedulerv2.Scheduler_AnnouncePeerServer) {
				assert := assert.New(t)
				peer.StoreAnnouncePeerStream(stream)
				peer.DeleteAnnouncePeerStream()
				_, loaded := peer.LoadAnnouncePeerStream()
				assert.Equal(loaded, false)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, stream)
		})
	}
}

func TestPeer_LoadPiece(t *testing.T) {
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
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

			peer.StorePiece(tc.piece)
			piece, loaded := peer.LoadPiece(tc.pieceNumber)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestPeer_StorePiece(t *testing.T) {
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
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

			peer.StorePiece(tc.piece)
			piece, loaded := peer.LoadPiece(tc.pieceNumber)
			tc.expect(t, piece, loaded)
		})
	}
}

func TestPeer_DeletePiece(t *testing.T) {
	tests := []struct {
		name        string
		piece       *Piece
		pieceNumber int32
		expect      func(t *testing.T, peer *Peer)
	}{
		{
			name:        "delete piece",
			piece:       mockPiece,
			pieceNumber: mockPiece.Number,
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				_, loaded := peer.LoadPiece(mockPiece.Number)
				assert.Equal(loaded, false)
			},
		},
		{
			name:        "delete key does not exist",
			piece:       mockPiece,
			pieceNumber: 0,
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				piece, loaded := peer.LoadPiece(mockPiece.Number)
				assert.Equal(loaded, true)
				assert.Equal(piece.Number, mockPiece.Number)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

			peer.StorePiece(tc.piece)
			peer.DeletePiece(tc.pieceNumber)
			tc.expect(t, peer)
		})
	}
}

func TestPeer_Parents(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "peer has no parents",
			expect: func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.Task.StorePeer(peer)
				assert.Equal(len(peer.Parents()), 0)
			},
		},
		{
			name: "peer has parents",
			expect: func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				if err := peer.Task.AddPeerEdge(seedPeer, peer); err != nil {
					t.Fatal(err)
				}

				assert.Equal(len(peer.Parents()), 1)
				assert.Equal(peer.Parents()[0].ID, mockSeedPeerID)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			seedPeer := NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, seedPeer, stream)
		})
	}
}

func TestPeer_Children(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer)
	}{
		{
			name: "peer has no children",
			expect: func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.Task.StorePeer(peer)
				assert.Equal(len(peer.Children()), 0)
			},
		},
		{
			name: "peer has children",
			expect: func(t *testing.T, peer *Peer, seedPeer *Peer, stream schedulerv1.Scheduler_ReportPieceResultServer) {
				assert := assert.New(t)
				peer.Task.StorePeer(peer)
				peer.Task.StorePeer(seedPeer)
				if err := peer.Task.AddPeerEdge(peer, seedPeer); err != nil {
					t.Fatal(err)
				}

				assert.Equal(len(peer.Children()), 1)
				assert.Equal(peer.Children()[0].ID, mockSeedPeerID)
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
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			seedPeer := NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, mockHost)
			tc.expect(t, peer, seedPeer, stream)
		})
	}
}

func TestPeer_DownloadTinyFile(t *testing.T) {
	testData := []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" +
		"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	mockServer := func(t *testing.T, peer *Peer) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert := assert.New(t)
			assert.NotNil(peer)
			assert.Equal(r.URL.Path, fmt.Sprintf("/download/%s/%s", peer.Task.ID[:3], peer.Task.ID))
			assert.Equal(r.URL.RawQuery, fmt.Sprintf("peerId=%s", peer.ID))

			rgs, err := nethttp.ParseRange(r.Header.Get(headers.Range), 128)
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
		name       string
		mockServer func(t *testing.T, peer *Peer) *httptest.Server
		expect     func(t *testing.T, peer *Peer)
	}{
		{
			name:       "download tiny file",
			mockServer: mockServer,
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ContentLength.Store(32)
				data, err := peer.DownloadTinyFile()
				assert.NoError(err)
				assert.Equal(testData[:32], data)
			},
		},
		{
			name:       "download tiny file with range",
			mockServer: mockServer,
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ContentLength.Store(32)
				peer.Range = &nethttp.Range{
					Start:  0,
					Length: 10,
				}
				data, err := peer.DownloadTinyFile()
				assert.NoError(err)
				assert.Equal(testData[:32], data)
			},
		},
		{
			name: "download tiny file failed because of http status code",
			mockServer: func(t *testing.T, peer *Peer) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
			},
			expect: func(t *testing.T, peer *Peer) {
				assert := assert.New(t)
				peer.Task.ID = "foobar"
				_, err := peer.DownloadTinyFile()
				assert.EqualError(err, "bad response status 404 Not Found")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)

			if tc.mockServer == nil {
				tc.mockServer = mockServer
			}

			s := tc.mockServer(t, peer)
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

			mockHost.IP = ip
			mockHost.DownloadPort = int32(port)
			tc.expect(t, peer)
		})
	}
}

func TestPeer_CalculatePriority(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, priority commonv2.Priority)
	}{
		{
			name: "peer has priority",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				priority := commonv2.Priority_LEVEL4
				peer.Priority = priority
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL4)
			},
		},
		{
			name: "get applications failed",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetApplications().Return(nil, errors.New("bas")).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL0)
			},
		},
		{
			name: "can not found applications",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetApplications().Return([]*managerv2.Application{}, nil).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL0)
			},
		},
		{
			name: "can not found matching application",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "baw",
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL0)
			},
		},
		{
			name: "can not found priority",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.Task.Application = "bae"
				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bae",
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL0)
			},
		},
		{
			name: "match the priority of application",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.Task.Application = "baz"
				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "baz",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL1,
						},
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL1)
			},
		},
		{
			name: "match the priority of url",
			mock: func(peer *Peer, md *configmocks.MockDynconfigInterfaceMockRecorder) {
				peer.Task.Application = "bak"
				peer.Task.URL = "example.com"
				md.GetApplications().Return([]*managerv2.Application{
					{
						Name: "bak",
						Priority: &managerv2.ApplicationPriority{
							Value: commonv2.Priority_LEVEL1,
							Urls: []*managerv2.URLPriority{
								{
									Regex: "am",
									Value: commonv2.Priority_LEVEL2,
								},
							},
						},
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, priority commonv2.Priority) {
				assert := assert.New(t)
				assert.Equal(priority, commonv2.Priority_LEVEL2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			peer := NewPeer(mockPeerID, mockResourceConfig, mockTask, mockHost)
			tc.mock(peer, dynconfig.EXPECT())
			tc.expect(t, peer.CalculatePriority(dynconfig))
		})
	}
}
