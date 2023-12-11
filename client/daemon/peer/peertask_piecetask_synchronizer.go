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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
)

type pieceTaskSyncManager struct {
	sync.RWMutex
	ctx               context.Context
	ctxCancel         context.CancelFunc
	peerTaskConductor *peerTaskConductor
	pieceRequestQueue PieceDispatcher
	workers           map[string]*pieceTaskSynchronizer
	watchdog          *synchronizerWatchdog
}

type pieceTaskSynchronizer struct {
	*logger.SugaredLoggerOnWith
	ctx               context.Context
	ctxCancel         context.CancelFunc
	span              trace.Span
	syncPiecesStream  dfdaemonv1.Daemon_SyncPieceTasksClient
	grpcClient        dfdaemonclient.V1
	dstPeer           *schedulerv1.PeerPacket_DestPeer
	error             atomic.Value
	grpcInitialized   *atomic.Bool
	grpcInitError     atomic.Value
	peerTaskConductor *peerTaskConductor
	pieceRequestQueue PieceDispatcher
}

type synchronizerWatchdog struct {
	done              chan struct{}
	mainPeer          atomic.Value // save *schedulerv1.PeerPacket_DestPeer
	syncSuccess       *atomic.Bool
	peerTaskConductor *peerTaskConductor
}

type pieceTaskSynchronizerError struct {
	err error
}

// FIXME for compatibility, sync will be called after the dfdaemonclient.GetPieceTasks deprecated and the pieceTaskPoller removed
func (s *pieceTaskSyncManager) syncPeers(destPeers []*schedulerv1.PeerPacket_DestPeer, desiredPiece int32) {
	s.Lock()
	defer func() {
		if s.peerTaskConductor.WatchdogTimeout > 0 {
			s.resetWatchdog(destPeers[0])
		}
		s.Unlock()
	}()

	peersToKeep, peersToAdd, peersToClose := s.diffPeers(destPeers)

	for _, peer := range peersToAdd {
		s.newPieceTaskSynchronizer(s.ctx, peer, desiredPiece)
	}

	for _, peer := range peersToKeep {
		worker := s.workers[peer.PeerId]
		// worker is working, keep it going on
		if worker.error.Load() == nil {
			s.peerTaskConductor.Infof("reuse working PieceTaskSynchronizer %s", peer.PeerId)
		} else {
			s.peerTaskConductor.Infof("close stale PieceTaskSynchronizer %s and re-initialize it", peer.PeerId)
			// clean error worker
			worker.close()
			delete(s.workers, peer.PeerId)
			// reconnect and retry
			s.newPieceTaskSynchronizer(s.ctx, peer, desiredPiece)
		}
	}

	// close stale workers
	for _, p := range peersToClose {
		s.workers[p].close()
		delete(s.workers, p)
	}

	return
}

func (s *pieceTaskSyncManager) diffPeers(peers []*schedulerv1.PeerPacket_DestPeer) (
	peersToKeep []*schedulerv1.PeerPacket_DestPeer, peersToAdd []*schedulerv1.PeerPacket_DestPeer, peersToClose []string) {
	if len(s.workers) == 0 {
		return nil, peers, nil
	}

	cache := make(map[string]bool)
	for _, p := range peers {
		cache[p.PeerId] = true
		if _, ok := s.workers[p.PeerId]; ok {
			peersToKeep = append(peersToKeep, p)
		} else {
			peersToAdd = append(peersToAdd, p)
		}
	}

	for p := range s.workers {
		if !cache[p] {
			peersToClose = append(peersToClose, p)
		}
	}
	return
}

func (s *pieceTaskSyncManager) newPieceTaskSynchronizer(
	ctx context.Context,
	dstPeer *schedulerv1.PeerPacket_DestPeer,
	desiredPiece int32) {
	_, span := tracer.Start(s.ctx, config.SpanSyncPieceTasks)
	span.SetAttributes(config.AttributeTargetPeerID.String(dstPeer.PeerId))
	request := &commonv1.PieceTaskRequest{
		TaskId:   s.peerTaskConductor.taskID,
		SrcPid:   s.peerTaskConductor.peerID,
		DstPid:   dstPeer.PeerId,
		StartNum: uint32(desiredPiece),
		Limit:    16,
	}
	ctx, cancel := context.WithCancel(ctx)
	synchronizer := &pieceTaskSynchronizer{
		ctx:                 ctx,
		ctxCancel:           cancel,
		span:                span,
		peerTaskConductor:   s.peerTaskConductor,
		pieceRequestQueue:   s.pieceRequestQueue,
		dstPeer:             dstPeer,
		error:               atomic.Value{},
		grpcInitialized:     atomic.NewBool(false),
		grpcInitError:       atomic.Value{},
		SugaredLoggerOnWith: s.peerTaskConductor.With("targetPeerID", request.DstPid),
	}
	s.workers[dstPeer.PeerId] = synchronizer
	go synchronizer.start(request, dstPeer)
	return
}

func (s *pieceTaskSyncManager) resetWatchdog(mainPeer *schedulerv1.PeerPacket_DestPeer) {
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
	go s.watchdog.watch(s.peerTaskConductor.WatchdogTimeout)
}

func compositePieceResult(peerTaskConductor *peerTaskConductor, destPeer *schedulerv1.PeerPacket_DestPeer, code commonv1.Code) *schedulerv1.PieceResult {
	return &schedulerv1.PieceResult{
		TaskId:        peerTaskConductor.taskID,
		SrcPid:        peerTaskConductor.peerID,
		DstPid:        destPeer.PeerId,
		PieceInfo:     &commonv1.PieceInfo{},
		Success:       false,
		Code:          code,
		FinishedCount: peerTaskConductor.readyPieces.Settled(),
	}
}

func (s *pieceTaskSyncManager) reportInvalidPeer(destPeer *schedulerv1.PeerPacket_DestPeer, code commonv1.Code) {
	sendError := s.peerTaskConductor.sendPieceResult(compositePieceResult(s.peerTaskConductor, destPeer, code))
	if sendError != nil {
		s.peerTaskConductor.Errorf("connect peer %s failed and send piece result with error: %s", destPeer.PeerId, sendError)
		go s.peerTaskConductor.cancel(commonv1.Code_SchedError, sendError.Error())
	} else {
		s.peerTaskConductor.Debugf("report invalid peer %s/%d to scheduler", destPeer.PeerId, code)
	}
}

// acquire send the target piece to other peers
func (s *pieceTaskSyncManager) acquire(request *commonv1.PieceTaskRequest) (attempt int, success int) {
	s.RLock()
	for _, p := range s.workers {
		attempt++
		if p.grpcInitialized.Load() && p.acquire(request) == nil {
			success++
		}
	}
	s.RUnlock()
	return
}

func (s *pieceTaskSyncManager) cancel() {
	s.ctxCancel()
	s.pieceRequestQueue.Close()
	s.Lock()
	for _, p := range s.workers {
		p.close()
	}
	s.workers = map[string]*pieceTaskSynchronizer{}
	s.Unlock()
}

func (s *pieceTaskSynchronizer) start(request *commonv1.PieceTaskRequest, dstPeer *schedulerv1.PeerPacket_DestPeer) {
	var startError error
	defer func() {
		if startError != nil {
			s.grpcInitError.Store(&pieceTaskSynchronizerError{startError})
			s.peerTaskConductor.Errorf("connect peer %s error: %s", dstPeer.PeerId, startError)
			if errors.Is(startError, context.DeadlineExceeded) {
				// connect timeout error, report to scheduler to get more available peers
				s.peerTaskConductor.pieceTaskSyncManager.reportInvalidPeer(dstPeer, commonv1.Code_ClientConnectionError)
			} else {
				// other errors, report to scheduler to get more available peers
				s.peerTaskConductor.pieceTaskSyncManager.reportInvalidPeer(dstPeer, commonv1.Code_ClientPieceRequestFail)
			}
		}
	}()

	formatIP, ok := ip.FormatIP(dstPeer.Ip)
	if !ok {
		startError = errors.New("format ip failed")
		return
	}

	netAddr := &dfnet.NetAddr{
		Type: dfnet.TCP,
		Addr: fmt.Sprintf("%s:%d", formatIP, dstPeer.RpcPort),
	}

	credentialOpt := grpc.WithTransportCredentials(s.peerTaskConductor.GRPCCredentials)

	dialCtx, cancel := context.WithTimeout(s.ctx, s.peerTaskConductor.GRPCDialTimeout)
	grpcClient, err := dfdaemonclient.GetV1(dialCtx, netAddr.String(), credentialOpt, grpc.WithBlock())
	cancel()

	if err != nil {
		startError = err
		return
	}

	stream, err := grpcClient.SyncPieceTasks(s.ctx, request)
	// Refer: https://github.com/grpc/grpc-go/blob/v1.44.0/stream.go#L104
	// When receive io.EOF, the real error should be discovered using RecvMsg, here is client.Recv()
	if err == io.EOF && stream != nil {
		_, err = stream.Recv()
	}
	if err != nil {
		// grpc client must be close, Refer: https://github.com/grpc/grpc-go/issues/5321
		_ = grpcClient.Close()
		if stream != nil {
			_ = stream.CloseSend()
		}
		s.peerTaskConductor.Errorf("call SyncPieceTasks error: %s, dest peer: %s", err, dstPeer.PeerId)
		startError = err
		return
	}

	s.syncPiecesStream = stream
	s.grpcClient = grpcClient

	s.grpcInitialized.Store(true)
	s.receive()
}

func (s *pieceTaskSynchronizer) close() {
	s.ctxCancel()
	if s.grpcInitialized.Load() {
		s.closeGRPC()
		s.Infof("pieceTaskSynchronizer grpc closed")
	} else {
		go s.waitAndClose()
	}
}

// one of grpcInitialized and grpcInitError must be true, otherwise the pieceTaskSynchronizer is initializing, wait it
func (s *pieceTaskSynchronizer) waitAndClose() {
	for {
		// grpc is ready, just close
		if s.grpcInitialized.Load() {
			s.closeGRPC()
			s.Infof("pieceTaskSynchronizer grpc closed and exit in background")
			return
		}
		// grpc init error
		if s.grpcInitError.Load() != nil {
			s.Infof("pieceTaskSynchronizer grpc init error and exit in background")
			return
		}
		s.Infof("pieceTaskSynchronizer grpc is initializing, wait it completed in background")
		time.Sleep(time.Minute)
	}
}

func (s *pieceTaskSynchronizer) closeGRPC() {
	if err := s.syncPiecesStream.CloseSend(); err != nil {
		s.error.Store(&pieceTaskSynchronizerError{err})
		s.Debugf("close send error: %s, dest peer: %s", err, s.dstPeer.PeerId)
		s.span.RecordError(err)
	}
	if err := s.grpcClient.Close(); err != nil {
		s.error.Store(&pieceTaskSynchronizerError{err})
		s.Debugf("close grpc client error: %s, dest peer: %s", err, s.dstPeer.PeerId)
		s.span.RecordError(err)
	}
	s.span.End()
}

func (s *pieceTaskSynchronizer) dispatchPieceRequest(piecePacket *commonv1.PiecePacket) {
	s.peerTaskConductor.updateMetadata(piecePacket)

	pieceCount := len(piecePacket.PieceInfos)
	s.Debugf("dispatch piece request, piece count: %d, dest peer: %s", pieceCount, s.dstPeer.PeerId)
	// peers maybe send zero piece info, but with total piece count and content length
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

		s.pieceRequestQueue.Put(req)
		s.span.AddEvent(fmt.Sprintf("send piece #%d request to piece download queue", piece.PieceNum))

		select {
		case <-s.peerTaskConductor.successCh:
			s.Infof("peer task success, stop dispatch piece request, dest peer: %s", s.dstPeer.PeerId)
		case <-s.peerTaskConductor.failCh:
			s.Warnf("peer task fail, stop dispatch piece request, dest peer: %s", s.dstPeer.PeerId)
		default:
		}
	}
}

func (s *pieceTaskSynchronizer) receive() {
	var (
		piecePacket *commonv1.PiecePacket
		err         error
	)
	for {
		piecePacket, err = s.syncPiecesStream.Recv()
		if err != nil {
			break
		}
		s.dispatchPieceRequest(piecePacket)
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
	}
}

func (s *pieceTaskSynchronizer) acquire(request *commonv1.PieceTaskRequest) error {
	if s.error.Load() != nil {
		err := s.error.Load().(*pieceTaskSynchronizerError).err
		s.Debugf("synchronizer already error %s, skip acquire more pieces", err)
		return err
	}
	request.DstPid = s.dstPeer.PeerId
	err := s.syncPiecesStream.Send(request)
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
	errCode := commonv1.Code_ClientPieceRequestFail

	// extract DfError for grpc status
	de, ok := dferrors.IsGRPCDfError(err)
	if ok {
		errCode = de.Code
		s.Errorf("report error with convert code from grpc error, code: %d, message: %s", de.Code, de.Message)
	}

	sendError := s.peerTaskConductor.sendPieceResult(compositePieceResult(s.peerTaskConductor, s.dstPeer, errCode))
	if sendError != nil {
		s.Errorf("sync piece info failed and send piece result with error: %s", sendError)
		go s.peerTaskConductor.cancel(commonv1.Code_SchedError, sendError.Error())
	} else {
		s.Debugf("report sync piece error to scheduler")
	}
}

func (s *pieceTaskSynchronizer) canceled(err error) bool {
	if errors.Is(err, context.Canceled) {
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
		s.peerTaskConductor, s.mainPeer.Load().(*schedulerv1.PeerPacket_DestPeer), commonv1.Code_ClientPieceRequestFail))
	if sendError != nil {
		s.peerTaskConductor.Errorf("watchdog sync piece info failed and send piece result with error: %s", sendError)
		go s.peerTaskConductor.cancel(commonv1.Code_SchedError, sendError.Error())
	} else {
		s.peerTaskConductor.Debugf("report watchdog sync piece error to scheduler")
	}
}
