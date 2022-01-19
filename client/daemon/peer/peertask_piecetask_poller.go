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

package peer

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type pieceTaskPoller struct {
	peerTaskConductor *peerTaskConductor
	// getPiecesMaxRetry stands max retry to get pieces from one peer packet
	getPiecesMaxRetry int
	peerTaskClient    dfclient.ElasticClient
}

func (poller *pieceTaskPoller) preparePieceTasks(request *base.PieceTaskRequest) (pp *base.PiecePacket, err error) {
	ptc := poller.peerTaskConductor
	defer ptc.recoverFromPanic()
	var retryCount int
prepare:
	retryCount++
	peerPacket := ptc.peerPacket.Load().(*scheduler.PeerPacket)
	ptc.pieceParallelCount.Store(peerPacket.ParallelCount)
	request.DstPid = peerPacket.MainPeer.PeerId
	pp, err = poller.preparePieceTasksByPeer(peerPacket, peerPacket.MainPeer, request)
	if err == nil {
		return
	}
	if err == errPeerPacketChanged {
		if poller.getPiecesMaxRetry > 0 && retryCount > poller.getPiecesMaxRetry {
			err = fmt.Errorf("get pieces max retry count reached")
			return
		}
		goto prepare
	}
	for _, peer := range peerPacket.StealPeers {
		request.DstPid = peer.PeerId
		pp, err = poller.preparePieceTasksByPeer(peerPacket, peer, request)
		if err == nil {
			return
		}
		if err == errPeerPacketChanged {
			if poller.getPiecesMaxRetry > 0 && retryCount > poller.getPiecesMaxRetry {
				err = fmt.Errorf("get pieces max retry count reached")
				return
			}
			goto prepare
		}
	}
	return
}

func (poller *pieceTaskPoller) preparePieceTasksByPeer(
	curPeerPacket *scheduler.PeerPacket,
	peer *scheduler.PeerPacket_DestPeer, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	ptc := poller.peerTaskConductor
	if peer == nil {
		return nil, fmt.Errorf("empty peer")
	}

	var span trace.Span
	_, span = tracer.Start(ptc.ctx, config.SpanGetPieceTasks)
	span.SetAttributes(config.AttributeTargetPeerID.String(peer.PeerId))
	span.SetAttributes(config.AttributeGetPieceStartNum.Int(int(request.StartNum)))
	span.SetAttributes(config.AttributeGetPieceLimit.Int(int(request.Limit)))
	defer span.End()

	// when cdn returns base.Code_CDNTaskNotFound, report it to scheduler and wait cdn download it.
retry:
	ptc.Debugf("try get piece task from peer %s, piece num: %d, limit: %d\"", peer.PeerId, request.StartNum, request.Limit)
	p, err := poller.getPieceTasksByPeer(span, curPeerPacket, peer, request)
	if err == nil {
		ptc.Infof("got piece task from peer %s ok, pieces length: %d", peer.PeerId, len(p.PieceInfos))
		span.SetAttributes(config.AttributeGetPieceCount.Int(len(p.PieceInfos)))
		return p, nil
	}
	span.RecordError(err)
	if err == errPeerPacketChanged {
		return nil, err
	}
	ptc.Debugf("get piece task error: %#v", err)

	// grpc error
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		ptc.Debugf("get piece task with grpc error, code: %d", se.GRPCStatus().Code())
		// context canceled, just exit
		if se.GRPCStatus().Code() == codes.Canceled {
			span.AddEvent("context canceled")
			ptc.Warnf("get piece task from peer %s canceled: %s", peer.PeerId, err)
			return nil, err
		}
	}
	code := base.Code_ClientPieceRequestFail
	// not grpc error
	if st, ok := status.FromError(err); ok && uint32(st.Code()) > uint32(codes.Unauthenticated) {
		ptc.Debugf("get piece task from peer %s with df error, code: %d", peer.PeerId, st.Code())
		code = base.Code(st.Code())
	}
	ptc.Errorf("get piece task from peer %s error: %s, code: %d", peer.PeerId, err, code)
	sendError := ptc.peerPacketStream.Send(&scheduler.PieceResult{
		TaskId:        ptc.taskID,
		SrcPid:        ptc.peerID,
		DstPid:        peer.PeerId,
		PieceInfo:     &base.PieceInfo{},
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: -1,
	})
	// error code should be sent to scheduler and the scheduler can schedule a new peer
	if sendError != nil {
		ptc.cancel(base.Code_SchedError, sendError.Error())
		span.RecordError(sendError)
		ptc.Errorf("send piece result error: %s, code to send: %d", sendError, code)
		return nil, sendError
	}

	// currently, before cdn gc tasks, it did not notify scheduler, when cdn complains Code_CDNTaskNotFound, retry
	if code == base.Code_CDNTaskNotFound && curPeerPacket == ptc.peerPacket.Load().(*scheduler.PeerPacket) {
		span.AddEvent("retry for CdnTaskNotFound")
		goto retry
	}
	return nil, err
}

func (poller *pieceTaskPoller) getPieceTasksByPeer(
	span trace.Span,
	curPeerPacket *scheduler.PeerPacket,
	peer *scheduler.PeerPacket_DestPeer,
	request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	var (
		peerPacketChanged bool
		count             int
		ptc               = poller.peerTaskConductor
	)
	p, _, err := retry.Run(ptc.ctx, func() (interface{}, bool, error) {
		piecePacket, getError := poller.peerTaskClient.GetPieceTasks(ptc.ctx, peer, request)
		// when GetPieceTasks returns err, exit retry
		if getError != nil {
			ptc.Errorf("get piece tasks with error: %s", getError)
			span.RecordError(getError)

			// fast way 1 to exit retry
			if st, ok := status.FromError(getError); ok {
				ptc.Debugf("get piece task with grpc error, code: %d", st.Code())
				// bad request, like invalid piece num, just exit
				if st.Code() == codes.Code(base.Code_BadRequest) {
					span.AddEvent("bad request")
					ptc.Warnf("get piece task from peer %s canceled: %s", peer.PeerId, getError)
					return nil, true, getError
				}
			}

			// fast way 2 to exit retry
			lastPeerPacket := ptc.peerPacket.Load().(*scheduler.PeerPacket)
			if curPeerPacket.MainPeer.PeerId != lastPeerPacket.MainPeer.PeerId {
				ptc.Warnf("get piece tasks with error: %s, but peer packet changed, switch to new peer packet, current destPeer %s, new destPeer %s", getError,
					curPeerPacket.MainPeer.PeerId, lastPeerPacket.MainPeer.PeerId)
				peerPacketChanged = true
				return nil, true, nil
			}
			return nil, true, getError
		}
		// got any pieces
		if len(piecePacket.PieceInfos) > 0 {
			return piecePacket, false, nil
		}
		// need update metadata
		if piecePacket.ContentLength > ptc.contentLength.Load() || piecePacket.TotalPiece > ptc.totalPiece {
			return piecePacket, false, nil
		}
		// invalid request num
		if piecePacket.TotalPiece > -1 && uint32(piecePacket.TotalPiece) <= request.StartNum {
			ptc.Warnf("invalid start num: %d, total piece: %d", request.StartNum, piecePacket.TotalPiece)
			return piecePacket, false, nil
		}

		// by santong: when peer return empty, retry later
		sendError := ptc.peerPacketStream.Send(&scheduler.PieceResult{
			TaskId:        ptc.taskID,
			SrcPid:        ptc.peerID,
			DstPid:        peer.PeerId,
			PieceInfo:     &base.PieceInfo{},
			Success:       false,
			Code:          base.Code_ClientWaitPieceReady,
			HostLoad:      nil,
			FinishedCount: ptc.readyPieces.Settled(),
		})
		if sendError != nil {
			ptc.cancel(base.Code_SchedError, sendError.Error())
			span.RecordError(sendError)
			ptc.Errorf("send piece result with base.Code_ClientWaitPieceReady error: %s", sendError)
			return nil, true, sendError
		}
		// fast way to exit retry
		lastPeerPacket := ptc.peerPacket.Load().(*scheduler.PeerPacket)
		if curPeerPacket.MainPeer.PeerId != lastPeerPacket.MainPeer.PeerId {
			ptc.Warnf("get empty pieces and peer packet changed, switch to new peer packet, current destPeer %s, new destPeer %s",
				curPeerPacket.MainPeer.PeerId, lastPeerPacket.MainPeer.PeerId)
			peerPacketChanged = true
			return nil, true, nil
		}
		count++
		span.AddEvent("retry due to empty pieces",
			trace.WithAttributes(config.AttributeGetPieceRetry.Int(count)))
		ptc.Infof("peer %s returns success but with empty pieces, retry later", peer.PeerId)
		return nil, false, dferrors.ErrEmptyValue
	}, 0.05, 0.2, 40, nil)
	if peerPacketChanged {
		return nil, errPeerPacketChanged
	}

	if err == nil {
		return p.(*base.PiecePacket), nil
	}
	return nil, err
}
