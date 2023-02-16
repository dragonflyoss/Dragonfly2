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
	"strings"
	"time"

	cdnsystemv1 "d7y.io/api/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
	"d7y.io/dragonfly/v2/scheduler/metrics"
)

const (
	// Default value of seed peer failed timeout.
	SeedPeerFailedTimeout = 30 * time.Minute
)

// SeedPeer is the interface used for seed peer.
type SeedPeer interface {
	// DownloadTask downloads task back-to-source.
	// Used only in v2 version of the grpc.
	DownloadTask(context.Context, *Task) error

	// TriggerTask triggers the seed peer to download task.
	// Used only in v1 version of the grpc.
	TriggerTask(context.Context, *http.Range, *Task) (*Peer, *schedulerv1.PeerResult, error)

	// Client returns grpc client of seed peer.
	Client() SeedPeerClient

	// Stop seed peer serivce.
	Stop() error
}

// seedPeer contains content for seed peer.
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

// TODO Implement DownloadTask
// DownloadTask downloads task back-to-source.
// Used only in v2 version of the grpc.
func (s *seedPeer) DownloadTask(ctx context.Context, task *Task) error {
	return nil
}

// TriggerTask triggers the seed peer to download task.
// Used only in v1 version of the grpc.
func (s *seedPeer) TriggerTask(ctx context.Context, rg *http.Range, task *Task) (*Peer, *schedulerv1.PeerResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	urlMeta := &commonv1.UrlMeta{
		Tag:         task.Tag,
		Filter:      strings.Join(task.Filters, idgen.URLFilterSeparator),
		Header:      task.Header,
		Application: task.Application,
		Priority:    commonv1.Priority_LEVEL0,
	}

	if task.Digest != nil {
		urlMeta.Digest = task.Digest.String()
	}

	if rg != nil {
		urlMeta.Range = rg.URLMetaString()
	}

	stream, err := s.client.ObtainSeeds(ctx, &cdnsystemv1.SeedRequest{
		TaskId:  task.ID,
		Url:     task.URL,
		UrlMeta: urlMeta,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		peer        *Peer
		initialized bool
	)

	for {
		pieceSeed, err := stream.Recv()
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
			peer, err = s.initSeedPeer(ctx, rg, task, pieceSeed)
			if err != nil {
				return nil, nil, err
			}
		}

		if pieceSeed.PieceInfo != nil {
			// Handle begin of piece.
			if pieceSeed.PieceInfo.PieceNum == common.BeginOfPiece {
				peer.Log.Infof("receive begin of piece from seed peer: %#v %#v", pieceSeed, pieceSeed.PieceInfo)
				if err := peer.FSM.Event(ctx, PeerEventDownload); err != nil {
					return nil, nil, err
				}

				continue
			}

			// Handle piece download successfully.
			peer.Log.Infof("receive piece from seed peer: %#v %#v", pieceSeed, pieceSeed.PieceInfo)
			cost := time.Duration(int64(pieceSeed.PieceInfo.DownloadCost) * int64(time.Millisecond))
			piece := &Piece{
				Number:      pieceSeed.PieceInfo.PieceNum,
				Offset:      pieceSeed.PieceInfo.RangeStart,
				Length:      uint64(pieceSeed.PieceInfo.RangeSize),
				TrafficType: commonv2.TrafficType_BACK_TO_SOURCE,
				Cost:        cost,
				CreatedAt:   time.Now().Add(-cost),
			}

			if len(pieceSeed.PieceInfo.PieceMd5) > 0 {
				piece.Digest = digest.New(digest.AlgorithmMD5, pieceSeed.PieceInfo.PieceMd5)
			}

			peer.FinishedPieces.Set(uint(pieceSeed.PieceInfo.PieceNum))
			peer.AppendPieceCost(piece.Cost)

			// When the piece is downloaded successfully,
			// peer.UpdatedAt needs to be updated to prevent
			// the peer from being GC during the download process.
			peer.UpdatedAt.Store(time.Now())
			peer.PieceUpdatedAt.Store(time.Now())
			task.StorePiece(piece)

			// Statistical traffic metrics.
			trafficType := commonv2.TrafficType_BACK_TO_SOURCE
			if pieceSeed.Reuse {
				trafficType = commonv2.TrafficType_REMOTE_PEER
			}
			metrics.Traffic.WithLabelValues(peer.Task.Tag, peer.Task.Application, trafficType.String()).Add(float64(pieceSeed.PieceInfo.RangeSize))
		}

		// Handle end of piece.
		if pieceSeed.Done {
			peer.Log.Infof("receive done piece")
			return peer, &schedulerv1.PeerResult{
				TotalPieceCount: pieceSeed.TotalPieceCount,
				ContentLength:   pieceSeed.ContentLength,
			}, nil
		}
	}
}

// Initialize seed peer.
func (s *seedPeer) initSeedPeer(ctx context.Context, rg *http.Range, task *Task, ps *cdnsystemv1.PieceSeed) (*Peer, error) {
	// Load peer from manager.
	peer, loaded := s.peerManager.Load(ps.PeerId)
	if loaded {
		return peer, nil
	}
	task.Log.Infof("can not find seed peer: %s", ps.PeerId)

	// Load host from manager.
	host, loaded := s.hostManager.Load(ps.HostId)
	if !loaded {
		task.Log.Errorf("can not find seed host id: %s", ps.HostId)
		return nil, fmt.Errorf("can not find host id: %s", ps.HostId)
	}

	options := []PeerOption{}
	if rg != nil {
		options = append(options, WithRange(*rg))
	}

	// New and store seed peer without range.
	peer = NewPeer(ps.PeerId, task, host, options...)
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
