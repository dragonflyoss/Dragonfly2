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

package rpcserver

import (
	"context"
	"fmt"
	"math"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type seeder struct {
	server *server
}

func (s *seeder) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return s.server.GetPieceTasks(ctx, request)
}

func (s *seeder) SyncPieceTasks(tasksServer cdnsystem.Seeder_SyncPieceTasksServer) error {
	return s.server.SyncPieceTasks(tasksServer)
}

func (s *seeder) ObtainSeeds(seedRequest *cdnsystem.SeedRequest, seedsServer cdnsystem.Seeder_ObtainSeedsServer) error {
	s.server.Keep()
	if seedRequest.UrlMeta == nil {
		seedRequest.UrlMeta = &base.UrlMeta{}
	}

	req := peer.SeedTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:         seedRequest.Url,
			UrlMeta:     seedRequest.UrlMeta,
			PeerId:      idgen.PeerID(s.server.peerHost.Ip),
			PeerHost:    s.server.peerHost,
			HostLoad:    nil,
			IsMigrating: false,
		},
		Limit:      0,
		Callsystem: "",
		Range:      nil,
	}

	log := logger.With("peer", req.PeerId, "component", "seedService")

	if len(req.UrlMeta.Range) > 0 {
		r, err := rangeutils.ParseRange(req.UrlMeta.Range, math.MaxInt)
		if err != nil {
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			log.Errorf(err.Error())
			return err
		}
		req.Range = &clientutil.Range{
			Start:  int64(r.StartIndex),
			Length: int64(r.Length()),
		}
	}

	err := seedsServer.Send(&cdnsystem.PieceSeed{
		PeerId:   req.PeerId,
		HostUuid: req.PeerHost.Uuid,
		PieceInfo: &base.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
		Done: false,
	})
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	pg, err := s.server.peerTaskManager.StartSeedTask(seedsServer.Context(), &req)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}
	ctx := seedsServer.Context()
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			log.Errorf(err.Error())
			return err
		case p := <-pg:
			ps := cdnsystem.PieceSeed{
				PeerId:   req.PeerId,
				HostUuid: req.PeerHost.Uuid,
				PieceInfo: &base.PieceInfo{
					PieceNum:     0,
					RangeStart:   0,
					RangeSize:    0,
					PieceMd5:     "",
					PieceOffset:  0,
					PieceStyle:   0,
					DownloadCost: 0,
				},
				Done:            false,
				ContentLength:   0,
				TotalPieceCount: 0,
				BeginTime:       0,
				EndTime:         0,
			}
			if err = seedsServer.Send(&ps); err != nil {
				log.Errorf(err.Error())
				return err
			}
			if p.PeerTaskDone {
				return nil
			}
		}
	}
}
