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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
)

type seeder struct {
	server *server
}

func (s *seeder) GetPieceTasks(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	return s.server.GetPieceTasks(ctx, request)
}

func (s *seeder) SyncPieceTasks(tasksServer cdnsystemv1.Seeder_SyncPieceTasksServer) error {
	return s.server.SyncPieceTasks(tasksServer)
}

func (s *seeder) ObtainSeeds(seedRequest *cdnsystemv1.SeedRequest, seedsServer cdnsystemv1.Seeder_ObtainSeedsServer) error {
	if logger.IsDebug() {
		printAuthInfo(seedsServer.Context())
	}

	metrics.SeedPeerConcurrentDownloadGauge.Inc()
	defer metrics.SeedPeerConcurrentDownloadGauge.Dec()
	metrics.SeedPeerDownloadCount.Add(1)

	s.server.Keep()
	if seedRequest.UrlMeta == nil {
		seedRequest.UrlMeta = &commonv1.UrlMeta{}
	}

	req := peer.SeedTaskRequest{
		PeerTaskRequest: schedulerv1.PeerTaskRequest{
			Url:         seedRequest.Url,
			UrlMeta:     seedRequest.UrlMeta,
			PeerId:      idgen.SeedPeerIDV1(s.server.peerHost.Ip), // when reuse peer task, peer id will be replaced.
			PeerHost:    s.server.peerHost,
			IsMigrating: false,
		},
		Limit: 0,
		Range: nil, // following code will update Range
	}

	log := logger.With("peer", req.PeerId, "task", seedRequest.TaskId, "component", "seedService")

	if len(req.UrlMeta.Range) > 0 {
		r, err := http.ParseURLMetaRange(req.UrlMeta.Range, math.MaxInt64)
		if err != nil {
			metrics.SeedPeerDownloadFailureCount.Add(1)
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			log.Errorf(err.Error())
			return err
		}
		req.Range = &http.Range{
			Start:  r.Start,
			Length: r.Length,
		}
	}

	resp, reuse, err := s.server.peerTaskManager.StartSeedTask(seedsServer.Context(), &req)
	if err != nil {
		metrics.SeedPeerDownloadFailureCount.Add(1)
		log.Errorf("start seed task error: %s", err.Error())
		return err
	}

	if resp.SubscribeResponse.Storage == nil {
		metrics.SeedPeerDownloadFailureCount.Add(1)
		err = fmt.Errorf("invalid SubscribeResponse.Storage")
		log.Errorf("%s", err.Error())
		return err
	}

	if resp.SubscribeResponse.Success == nil && resp.SubscribeResponse.Fail == nil {
		metrics.SeedPeerDownloadFailureCount.Add(1)
		err = fmt.Errorf("both of SubscribeResponse.Success and SubscribeResponse.Fail is nil")
		log.Errorf("%s", err.Error())
		return err
	}

	log.Infof("start seed task")

	err = seedsServer.Send(
		&cdnsystemv1.PieceSeed{
			PeerId: resp.PeerID,
			HostId: req.PeerHost.Id,
			PieceInfo: &commonv1.PieceInfo{
				PieceNum: common.BeginOfPiece,
			},
			Done: false,
		})
	if err != nil {
		metrics.SeedPeerDownloadFailureCount.Add(1)
		resp.Span.RecordError(err)
		log.Errorf("send piece seed error: %s", err.Error())
		return err
	}

	sync := seedSynchronizer{
		SeedTaskResponse:    resp,
		SugaredLoggerOnWith: log,
		seedsServer:         seedsServer,
		seedTaskRequest:     &req,
		startNanoSecond:     time.Now().UnixNano(),
	}
	defer resp.Span.End()

	if err := sync.sendPieceSeeds(reuse); err != nil {
		metrics.SeedPeerDownloadFailureCount.Add(1)
		return err
	}

	return nil
}

type seedSynchronizer struct {
	*peer.SeedTaskResponse
	*logger.SugaredLoggerOnWith
	seedsServer     cdnsystemv1.Seeder_ObtainSeedsServer
	seedTaskRequest *peer.SeedTaskRequest
	startNanoSecond int64
	attributeSent   bool
}

func (s *seedSynchronizer) sendPieceSeeds(reuse bool) (err error) {
	var (
		ctx           = s.Context
		desired       int32
		contentLength int64
	)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			s.Errorf("context done due to %s", err.Error())
			s.Span.RecordError(err)
			s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			return err
		case <-s.Success:
			s.Infof("seed task success, send reminding piece seeds")
			err = s.sendRemindingPieceSeeds(desired, reuse)
			if err != nil {
				s.Span.RecordError(err)
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			} else {
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(true))
			}
			return err
		case <-s.Fail:
			reason := s.FailReason()
			s.Errorf("seed task failed: %s", reason)
			s.Span.RecordError(err)
			s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			// return underlay status to scheduler
			if st, ok := status.FromError(reason); ok {
				return st.Err()
			}
			return status.Errorf(codes.Internal, "seed task failed: %s", reason)
		case p := <-s.PieceInfoChannel:
			s.Infof("receive piece info, num: %d, ordered num: %d, finish: %v", p.Num, p.OrderedNum, p.Finished)
			contentLength, desired, err = s.sendOrderedPieceSeeds(desired, p.OrderedNum, p.Finished, reuse)
			if err != nil {
				s.Span.RecordError(err)
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
				return err
			}
			if p.Finished {
				s.Debugf("send piece seeds finished")
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(true))
				s.updateMetric(reuse, contentLength)
				return nil
			}
		}
	}
}

func (s *seedSynchronizer) sendRemindingPieceSeeds(desired int32, reuse bool) error {
	for {
		pp, err := s.Storage.GetPieces(s.Context,
			&commonv1.PieceTaskRequest{
				TaskId:   s.TaskID,
				StartNum: uint32(desired),
				Limit:    16,
			})
		if err != nil {
			s.Errorf("get pieces error %s, desired: %d", err.Error(), desired)
			return err
		}
		if !s.attributeSent {
			exa, err := s.Storage.GetExtendAttribute(s.Context, nil)
			if err != nil {
				s.Errorf("get extend attribute error: %s", err.Error())
				return err
			}
			pp.ExtendAttribute = exa
			s.attributeSent = true
		}

		// we must send done to scheduler
		if len(pp.PieceInfos) == 0 {
			ps := s.compositePieceSeed(pp, nil, reuse)
			ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
			s.Infof("seed tasks start time: %d, end time: %d, cost: %dms", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000)
			err = s.seedsServer.Send(&ps)
			if err != nil {
				s.Errorf("send reminding piece seeds error: %s", err.Error())
				return err
			}
		}

		for _, p := range pp.PieceInfos {
			if p.PieceNum != desired {
				s.Errorf("desired piece %d, not found", desired)
				return status.Errorf(codes.Internal, "seed task piece %d not found", desired)
			}
			ps := s.compositePieceSeed(pp, p, reuse)
			if p.PieceNum == pp.TotalPiece-1 {
				ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
				s.Infof("seed tasks start time: %d, end time: %d, cost: %dms, piece number: %d", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000, p.PieceNum)
			}

			err = s.seedsServer.Send(&ps)
			if err != nil {
				s.Errorf("send reminding piece seeds error: %s", err.Error())
				return err
			}

			s.Span.AddEvent(fmt.Sprintf("send piece %d ok", desired))
			desired++
		}
		if desired == pp.TotalPiece {
			s.updateMetric(reuse, pp.ContentLength)
			s.Debugf("send reminding piece seeds ok")
			return nil
		}
	}
}

func (s *seedSynchronizer) sendOrderedPieceSeeds(desired, orderedNum int32, finished bool, reuse bool) (int64, int32, error) {
	cur := desired
	var contentLength int64 = -1
	for ; cur <= orderedNum; cur++ {
		pp, err := s.Storage.GetPieces(s.Context,
			&commonv1.PieceTaskRequest{
				TaskId:   s.TaskID,
				StartNum: uint32(cur),
				Limit:    1,
			})
		if err != nil {
			s.Errorf("get pieces error %s, desired: %d", err.Error(), cur)
			return -1, -1, err
		}
		if len(pp.PieceInfos) < 1 {
			s.Errorf("desired pieces %d not found", cur)
			return -1, -1, fmt.Errorf("get seed piece %d info failed", cur)
		}
		if !s.attributeSent {
			exa, err := s.Storage.GetExtendAttribute(s.Context, nil)
			if err != nil {
				s.Errorf("get extend attribute error: %s", err.Error())
				return -1, -1, err
			}
			pp.ExtendAttribute = exa
			s.attributeSent = true
		}

		ps := s.compositePieceSeed(pp, pp.PieceInfos[0], reuse)
		if cur == orderedNum && finished {
			ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
			s.Infof("seed tasks start time: %d, end time: %d, cost: %dms", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000)
		}
		err = s.seedsServer.Send(&ps)
		if err != nil {
			s.Errorf("send ordered piece seeds error: %s", err.Error())
			return -1, -1, err
		}
		s.Debugf("send piece %d seeds ok", cur)
		s.Span.AddEvent(fmt.Sprintf("send piece %d ok", cur))
		contentLength = pp.ContentLength
	}
	return contentLength, cur, nil
}

func (s *seedSynchronizer) compositePieceSeed(pp *commonv1.PiecePacket, piece *commonv1.PieceInfo, reuse bool) cdnsystemv1.PieceSeed {
	return cdnsystemv1.PieceSeed{
		Reuse:           reuse,
		PeerId:          s.seedTaskRequest.PeerId,
		HostId:          s.seedTaskRequest.PeerHost.Id,
		PieceInfo:       piece,
		ContentLength:   pp.ContentLength,
		TotalPieceCount: pp.TotalPiece,
		BeginTime:       uint64(s.startNanoSecond),
		EndTime:         uint64(time.Now().UnixNano()),
	}
}

func (s *seedSynchronizer) updateMetric(reuse bool, contentLength int64) {
	seedPeerDownloadType := metrics.SeedPeerDownloadTypeBackToSource
	if reuse {
		seedPeerDownloadType = metrics.SeedPeerDownloadTypeP2P
	}
	if contentLength == -1 {
		s.Warnf("seed task content length is unknown, did not update metric")
		return
	}
	metrics.SeedPeerDownloadTraffic.WithLabelValues(seedPeerDownloadType).Add(float64(contentLength))
}
