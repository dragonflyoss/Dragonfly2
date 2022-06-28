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
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type pieceTaskSyncManager struct {
	sync.RWMutex
	ctx               context.Context
	ctxCancel         context.CancelFunc
	peerTaskConductor *peerTaskConductor
	pieceRequestCh    chan *DownloadPieceRequest
	workers           map[string]*pieceTaskSynchronizer
	watchdog          *synchronizerWatchdog
}

type pieceTaskSynchronizer struct {
	*logger.SugaredLoggerOnWith
	span              trace.Span
	client            dfdaemon.Daemon_SyncPieceTasksClient
	dstPeer           *scheduler.PeerPacket_DestPeer
	error             atomic.Value
	peerTaskConductor *peerTaskConductor
	pieceRequestCh    chan *DownloadPieceRequest
}

type synchronizerWatchdog struct {
	done              chan struct{}
	mainPeer          atomic.Value // save *scheduler.PeerPacket_DestPeer
	syncSuccess       *atomic.Bool
	peerTaskConductor *peerTaskConductor
}

type pieceTaskSynchronizerError struct {
	err error
}

// FIXME for compatibility, sync will be called after the dfclient.GetPieceTasks deprecated and the pieceTaskPoller removed
func (s *pieceTaskSyncManager) sync(pp *scheduler.PeerPacket, desiredPiece int32) error {
	var (
		peers = map[string]bool{}
		errs  []error
	)
	peers[pp.MainPeer.PeerId] = true
	// TODO if the worker failed, reconnect and retry
	s.Lock()
	defer s.Unlock()
	if _, ok := s.workers[pp.MainPeer.PeerId]; !ok {
		err := s.newPieceTaskSynchronizer(s.ctx, pp.MainPeer, desiredPiece)
		if err != nil {
			s.peerTaskConductor.Errorf("main peer SyncPieceTasks error: %s", err)
			errs = append(errs, err)
		}
	}
	for _, p := range pp.StealPeers {
		peers[p.PeerId] = true
		if _, ok := s.workers[p.PeerId]; !ok {
			err := s.newPieceTaskSynchronizer(s.ctx, p, desiredPiece)
			if err != nil {
				s.peerTaskConductor.Errorf("steal peer SyncPieceTasks error: %s", err)
				errs = append(errs, err)
			}
		}
	}

	// cancel old workers
	if len(s.workers) != len(peers) {
		var peersToRemove []string
		for p, worker := range s.workers {
			if !peers[p] {
				worker.close()
				peersToRemove = append(peersToRemove, p)
			}
		}
		for _, p := range peersToRemove {
			delete(s.workers, p)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (s *pieceTaskSyncManager) cleanStaleWorker(destPeers []*scheduler.PeerPacket_DestPeer) {
	var (
		peers = map[string]bool{}
	)
	for _, p := range destPeers {
		peers[p.PeerId] = true
	}

	// cancel old workers
	if len(s.workers) != len(peers) {
		var peersToRemove []string
		for p, worker := range s.workers {
			if !peers[p] {
				worker.close()
				peersToRemove = append(peersToRemove, p)
			}
		}
		for _, p := range peersToRemove {
			delete(s.workers, p)
		}
	}
}

func (s *pieceTaskSyncManager) newPieceTaskSynchronizer(
	ctx context.Context,
	dstPeer *scheduler.PeerPacket_DestPeer,
	desiredPiece int32) error {
	request := &base.PieceTaskRequest{
		TaskId:   s.peerTaskConductor.taskID,
		SrcPid:   s.peerTaskConductor.peerID,
		DstPid:   dstPeer.PeerId,
		StartNum: uint32(desiredPiece),
		Limit:    16,
	}
	if worker, ok := s.workers[dstPeer.PeerId]; ok {
		// worker is okay, keep it go on
		if worker.error.Load() == nil {
			s.peerTaskConductor.Infof("reuse PieceTaskSynchronizer %s", dstPeer.PeerId)
			return nil
		}
		// clean error worker
		delete(s.workers, dstPeer.PeerId)
	}

	client, err := dfclient.SyncPieceTasks(ctx, dstPeer, request)
	// Refer: https://github.com/grpc/grpc-go/blob/v1.44.0/stream.go#L104
	// When receive io.EOF, the real error should be discovered using RecvMsg, here is client.Recv() here
	if err == io.EOF && client != nil {
		_, err = client.Recv()
	}
	if err != nil {
		s.peerTaskConductor.Errorf("call SyncPieceTasks error: %s, dest peer: %s", err, dstPeer.PeerId)
		return err
	}

	// TODO the codes.Unimplemented is received only in client.Recv()
	// when remove legacy get piece grpc, can move this check into synchronizer.receive
	piecePacket, err := client.Recv()
	if err != nil {
		s.peerTaskConductor.Warnf("receive from SyncPieceTasksClient error: %s, dest peer: %s", err, dstPeer.PeerId)
		_ = client.CloseSend()
		return err
	}

	_, span := tracer.Start(s.ctx, config.SpanSyncPieceTasks)
	span.SetAttributes(config.AttributeTargetPeerID.String(dstPeer.PeerId))
	synchronizer := &pieceTaskSynchronizer{
		span:                span,
		peerTaskConductor:   s.peerTaskConductor,
		pieceRequestCh:      s.pieceRequestCh,
		client:              client,
		dstPeer:             dstPeer,
		error:               atomic.Value{},
		SugaredLoggerOnWith: s.peerTaskConductor.With("targetPeerID", request.DstPid),
	}
	s.workers[dstPeer.PeerId] = synchronizer
	go synchronizer.receive(piecePacket)
	return nil
}

func (s *pieceTaskSyncManager) newMultiPieceTaskSynchronizer(
	destPeers []*scheduler.PeerPacket_DestPeer,
	desiredPiece int32) (legacyPeers []*scheduler.PeerPacket_DestPeer) {
	s.Lock()
	defer func() {
		if s.peerTaskConductor.ptm.watchdogTimeout > 0 {
			s.resetWatchdog(destPeers[0])
		}
		s.Unlock()
	}()

	for _, peer := range destPeers {
		err := s.newPieceTaskSynchronizer(s.ctx, peer, desiredPiece)
		if err == nil {
			s.peerTaskConductor.Infof("connected to peer: %s", peer.PeerId)
			continue
		}
		// when err is codes.Unimplemented, fallback to legacy get piece grpc
		stat, ok := status.FromError(err)
		if ok && stat.Code() == codes.Unimplemented {
			// for legacy peers, when get pieces error, will report the error
			s.peerTaskConductor.Warnf("connect peer %s error: %s, fallback to legacy get piece grpc", peer.PeerId, err)
			legacyPeers = append(legacyPeers, peer)
			continue
		}

		// other errors, report to scheduler
		if errors.Is(err, context.DeadlineExceeded) {
			// connect timeout error, report to scheduler to get more available peers
			s.reportInvalidPeer(peer, base.Code_ClientConnectionError)
			s.peerTaskConductor.Infof("connect to peer %s with error: %s, peer is invalid, skip legacy grpc", peer.PeerId, err)
		} else {
			// other errors, report to scheduler to get more available peers
			s.reportInvalidPeer(peer, base.Code_ClientPieceRequestFail)
			s.peerTaskConductor.Errorf("connect peer %s error: %s, not codes.Unimplemented", peer.PeerId, err)
		}
	}
	s.cleanStaleWorker(destPeers)
	return legacyPeers
}

func (s *pieceTaskSyncManager) resetWatchdog(mainPeer *scheduler.PeerPacket_DestPeer) {
	if s.watchdog != nil {
		close(s.watchdog.done)
		s.peerTaskConductor.Debugf("close old watchdog")
	}
	s.watchdog = &synchronizerWatchdog{
		done:              make(chan struct{}),
		mainPeer:          atomic.Value{},
		syncSuccess:       atomic.NewBool(false),
		peerTaskConductor: s.peerTaskConductor,
	}
	s.watchdog.mainPeer.Store(mainPeer)
	s.peerTaskConductor.Infof("start new watchdog")
	go s.watchdog.watch(s.peerTaskConductor.ptm.watchdogTimeout)
}

func compositePieceResult(peerTaskConductor *peerTaskConductor, destPeer *scheduler.PeerPacket_DestPeer, code base.Code) *scheduler.PieceResult {
	return &scheduler.PieceResult{
		TaskId:        peerTaskConductor.taskID,
		SrcPid:        peerTaskConductor.peerID,
		DstPid:        destPeer.PeerId,
		PieceInfo:     &base.PieceInfo{},
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: peerTaskConductor.readyPieces.Settled(),
	}
}

func (s *pieceTaskSyncManager) reportInvalidPeer(destPeer *scheduler.PeerPacket_DestPeer, code base.Code) {
	sendError := s.peerTaskConductor.sendPieceResult(compositePieceResult(s.peerTaskConductor, destPeer, code))
	if sendError != nil {
		s.peerTaskConductor.Errorf("connect peer %s failed and send piece result with error: %s", destPeer.PeerId, sendError)
		go s.peerTaskConductor.cancel(base.Code_SchedError, sendError.Error())
	} else {
		s.peerTaskConductor.Debugf("report invalid peer %s/%d to scheduler", destPeer.PeerId, code)
	}
}

// acquire send the target piece to other peers
func (s *pieceTaskSyncManager) acquire(request *base.PieceTaskRequest) (attempt int, success int) {
	s.RLock()
	for _, p := range s.workers {
		attempt++
		if p.acquire(request) == nil {
			success++
		}
	}
	s.RUnlock()
	return
}

func (s *pieceTaskSyncManager) cancel() {
	s.ctxCancel()
	s.Lock()
	for _, p := range s.workers {
		p.close()
	}
	s.workers = map[string]*pieceTaskSynchronizer{}
	s.Unlock()
}

func (s *pieceTaskSynchronizer) close() {
	if err := s.client.CloseSend(); err != nil {
		s.error.Store(&pieceTaskSynchronizerError{err})
		s.Debugf("close send error: %s, dest peer: %s", err, s.dstPeer.PeerId)
		s.span.RecordError(err)
	}
	s.span.End()
}

func (s *pieceTaskSynchronizer) dispatchPieceRequest(piecePacket *base.PiecePacket) {
	s.peerTaskConductor.updateMetadata(piecePacket)

	pieceCount := len(piecePacket.PieceInfos)
	s.Debugf("dispatch piece request, piece count: %d, dest peer: %s", pieceCount, s.dstPeer.PeerId)
	// fix cdn return zero piece info, but with total piece count and content length
	if pieceCount == 0 {
		finished := s.peerTaskConductor.isCompleted()
		if finished {
			s.peerTaskConductor.Done()
		}
		return
	}
	for _, piece := range piecePacket.PieceInfos {
		s.Infof("got piece %d from %s/%s, digest: %s, start: %d, size: %d",
			piece.PieceNum, piecePacket.DstAddr, piecePacket.DstPid, piece.PieceMd5, piece.RangeStart, piece.RangeSize)
		// FIXME when set total piece but no total digest, fetch again
		s.peerTaskConductor.requestedPiecesLock.Lock()
		if !s.peerTaskConductor.requestedPieces.IsSet(piece.PieceNum) {
			s.peerTaskConductor.requestedPieces.Set(piece.PieceNum)
		}
		s.peerTaskConductor.requestedPiecesLock.Unlock()
		req := &DownloadPieceRequest{
			storage: s.peerTaskConductor.GetStorage(),
			piece:   piece,
			log:     s.peerTaskConductor.Log(),
			TaskID:  s.peerTaskConductor.GetTaskID(),
			PeerID:  s.peerTaskConductor.GetPeerID(),
			DstPid:  piecePacket.DstPid,
			DstAddr: piecePacket.DstAddr,
		}
		select {
		case s.pieceRequestCh <- req:
			s.span.AddEvent(fmt.Sprintf("send piece #%d request to piece download queue", piece.PieceNum))
		case <-s.peerTaskConductor.successCh:
			s.Infof("peer task success, stop dispatch piece request, dest peer: %s", s.dstPeer.PeerId)
		case <-s.peerTaskConductor.failCh:
			s.Warnf("peer task fail, stop dispatch piece request, dest peer: %s", s.dstPeer.PeerId)
		}
	}
}

func (s *pieceTaskSynchronizer) receive(piecePacket *base.PiecePacket) {
	var err error
	for {
		s.dispatchPieceRequest(piecePacket)
		piecePacket, err = s.client.Recv()
		if err != nil {
			break
		}
	}

	if err == io.EOF {
		s.Debugf("synchronizer receives io.EOF")
	} else if s.canceled(err) {
		s.Debugf("synchronizer receives canceled")
		s.error.Store(&pieceTaskSynchronizerError{err})
	} else {
		s.Errorf("synchronizer receives with error: %s", err)
		s.error.Store(&pieceTaskSynchronizerError{err})
		s.reportError(err)
		s.Errorf("synchronizer receives with error: %s", err)
	}
}

func (s *pieceTaskSynchronizer) acquire(request *base.PieceTaskRequest) error {
	if s.error.Load() != nil {
		err := s.error.Load().(*pieceTaskSynchronizerError).err
		s.Debugf("synchronizer already error %s, skip acquire more pieces", err)
		return err
	}
	request.DstPid = s.dstPeer.PeerId
	err := s.client.Send(request)
	s.span.AddEvent(fmt.Sprintf("send piece #%d request", request.StartNum))
	if err != nil {
		// send should always ok
		s.error.Store(&pieceTaskSynchronizerError{err})
		s.Errorf("synchronizer sends with error: %s", err)
		s.reportError(err)
	}
	return err
}

func (s *pieceTaskSynchronizer) reportError(err error) {
	s.span.RecordError(err)
	sendError := s.peerTaskConductor.sendPieceResult(compositePieceResult(s.peerTaskConductor, s.dstPeer, base.Code_ClientPieceRequestFail))
	if sendError != nil {
		s.Errorf("sync piece info failed and send piece result with error: %s", sendError)
		go s.peerTaskConductor.cancel(base.Code_SchedError, sendError.Error())
	} else {
		s.Debugf("report sync piece error to scheduler")
	}
}

func (s *pieceTaskSynchronizer) canceled(err error) bool {
	if err == context.Canceled {
		s.Debugf("context canceled, dst peer: %s", s.dstPeer.PeerId)
		return true
	}
	if stat, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		if stat.GRPCStatus().Code() == codes.Canceled {
			s.Debugf("grpc canceled, dst peer: %s", s.dstPeer.PeerId)
			return true
		}
	}
	return false
}

func (s *synchronizerWatchdog) watch(timeout time.Duration) {
	select {
	case <-time.After(timeout):
		if s.peerTaskConductor.readyPieces.Settled() == 0 {
			s.peerTaskConductor.Warnf("watch sync pieces timeout, may be a bug, " +
				"please file a issue in https://github.com/dragonflyoss/Dragonfly2/issues")
			s.syncSuccess.Store(false)
			s.reportWatchFailed()
		} else {
			s.peerTaskConductor.Infof("watch sync pieces ok")
		}
	case <-s.peerTaskConductor.successCh:
		s.peerTaskConductor.Debugf("peer task success, watchdog exit")
	case <-s.peerTaskConductor.failCh:
		s.peerTaskConductor.Debugf("peer task fail, watchdog exit")
	case <-s.done:
		s.peerTaskConductor.Debugf("watchdog done, exit")
	}
}

func (s *synchronizerWatchdog) reportWatchFailed() {
	sendError := s.peerTaskConductor.sendPieceResult(compositePieceResult(
		s.peerTaskConductor, s.mainPeer.Load().(*scheduler.PeerPacket_DestPeer), base.Code_ClientPieceRequestFail))
	if sendError != nil {
		s.peerTaskConductor.Errorf("watchdog sync piece info failed and send piece result with error: %s", sendError)
		go s.peerTaskConductor.cancel(base.Code_SchedError, sendError.Error())
	} else {
		s.peerTaskConductor.Debugf("report watchdog sync piece error to scheduler")
	}
}
