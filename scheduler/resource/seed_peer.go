/*
 *     Copyright 2022 The Dragonfly Authors
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

//go:generate mockgen -destination seed_peer_mock.go -source seed_peer.go -package resource

package resource

import (
	"context"
	"fmt"
	"time"

	cdnsystemv1 "d7y.io/api/pkg/apis/cdnsystem/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/pkg/rpc/common"
	pkgtime "d7y.io/dragonfly/v2/pkg/time"
	"d7y.io/dragonfly/v2/scheduler/metrics"
)

const (
	// SeedTag Default value of tag label for seed peer.
	SeedTag = "d7y/seed"

	// SeedApplication Default value of application label for seed peer.
	SeedApplication = "d7y/seed"
)

const (
	// Default value of seed peer failed timeout.
	SeedPeerFailedTimeout = 30 * time.Minute
)

type SeedPeer interface {
	// TriggerTask triggers the seed peer to download the task.
	TriggerTask(context.Context, *Task) (*Peer, *schedulerv1.PeerResult, error)

	// Client returns grpc client of seed peer.
	Client() SeedPeerClient

	// Stop seed peer serivce.
	Stop() error
}

type seedPeer struct {
	// client is the dynamic client of seed peer.
	client SeedPeerClient
	// peerManager is PeerManager interface.
	peerManager PeerManager
	// hostManager is HostManager interface.
	hostManager HostManager
}

// New SeedPeer interface.
func newSeedPeer(client SeedPeerClient, peerManager PeerManager, hostManager HostManager) SeedPeer {
	return &seedPeer{
		client:      client,
		peerManager: peerManager,
		hostManager: hostManager,
	}
}

// TriggerTask start to trigger seed peer task.
func (s *seedPeer) TriggerTask(ctx context.Context, task *Task) (*Peer, *schedulerv1.PeerResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := s.client.ObtainSeeds(ctx, &cdnsystemv1.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: task.URLMeta,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		peer        *Peer
		initialized bool
	)

	for {
		piece, err := stream.Recv()
		if err != nil {
			// If the peer initialization succeeds and the download fails,
			// set peer status is PeerStateFailed.
			if peer != nil {
				if err := peer.FSM.Event(ctx, PeerEventDownloadFailed); err != nil {
					return nil, nil, err
				}
			}

			return nil, nil, err
		}

		if !initialized {
			initialized = true

			// Initialize seed peer.
			peer, err = s.initSeedPeer(ctx, task, piece)
			if err != nil {
				return nil, nil, err
			}
		}

		if piece.PieceInfo != nil {
			// Handle begin of piece.
			if piece.PieceInfo.PieceNum == common.BeginOfPiece {
				peer.Log.Infof("receive begin of piece from seed peer: %#v %#v", piece, piece.PieceInfo)
				if err := peer.FSM.Event(ctx, PeerEventDownload); err != nil {
					return nil, nil, err
				}

				continue
			}

			// Handle piece download successfully.
			peer.Log.Infof("receive piece from seed peer: %#v %#v", piece, piece.PieceInfo)
			peer.Pieces.Add(&schedulerv1.PieceResult{
				TaskId:          task.ID,
				SrcPid:          peer.ID,
				BeginTime:       piece.BeginTime,
				EndTime:         piece.EndTime,
				Success:         true,
				PieceInfo:       piece.PieceInfo,
				ExtendAttribute: piece.ExtendAttribute,
			})
			peer.FinishedPieces.Set(uint(piece.PieceInfo.PieceNum))
			peer.AppendPieceCost(pkgtime.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())

			// When the piece is downloaded successfully,
			// peer.UpdatedAt needs to be updated to prevent
			// the peer from being GC during the download process.
			peer.UpdatedAt.Store(time.Now())
			peer.PieceUpdatedAt.Store(time.Now())
			task.StorePiece(piece.PieceInfo)

			// Statistical traffic metrics.
			trafficType := metrics.TrafficBackToSourceType
			if piece.Reuse {
				trafficType = metrics.TrafficP2PType
			}
			metrics.Traffic.WithLabelValues(peer.Tag, peer.Application, trafficType).Add(float64(piece.PieceInfo.RangeSize))
		}

		// Handle end of piece.
		if piece.Done {
			peer.Log.Infof("receive done piece")
			return peer, &schedulerv1.PeerResult{
				TotalPieceCount: piece.TotalPieceCount,
				ContentLength:   piece.ContentLength,
			}, nil
		}
	}
}

// Initialize seed peer.
func (s *seedPeer) initSeedPeer(ctx context.Context, task *Task, ps *cdnsystemv1.PieceSeed) (*Peer, error) {
	// Load peer from manager.
	peer, ok := s.peerManager.Load(ps.PeerId)
	if ok {
		return peer, nil
	}
	task.Log.Infof("can not find seed peer: %s", ps.PeerId)

	// Load host from manager.
	host, ok := s.hostManager.Load(ps.HostId)
	if !ok {
		task.Log.Errorf("can not find seed host id: %s", ps.HostId)
		return nil, fmt.Errorf("can not find host id: %s", ps.HostId)
	}

	// New and store seed peer.
	peer = NewPeer(ps.PeerId, task, host, WithTag(SeedTag), WithApplication(SeedApplication))
	s.peerManager.Store(peer)
	peer.Log.Info("seed peer has been stored")

	if err := peer.FSM.Event(ctx, PeerEventRegisterNormal); err != nil {
		return nil, err
	}

	return peer, nil
}

// Client is seed peer grpc client.
func (s *seedPeer) Client() SeedPeerClient {
	return s.client
}

// Stop seed peer serivce.
func (s *seedPeer) Stop() error {
	return s.client.Close()
}
