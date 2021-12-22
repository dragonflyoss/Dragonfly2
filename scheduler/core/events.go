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

package core

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/util/workqueue"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

type event interface {
	hashKey() string
	apply(s *state)
}

type rsPeer struct {
	times     int32
	peer      *supervisor.Peer
	blocklist set.SafeSet
}

type state struct {
	sched                       Scheduler
	peerManager                 supervisor.PeerManager
	cdn                         supervisor.CDN
	waitScheduleParentPeerQueue workqueue.DelayingInterface
}

func newState(sched Scheduler, peerManager supervisor.PeerManager, cdn supervisor.CDN, wsdq workqueue.DelayingInterface) *state {
	return &state{
		sched:                       sched,
		peerManager:                 peerManager,
		cdn:                         cdn,
		waitScheduleParentPeerQueue: wsdq,
	}
}

type reScheduleParentEvent struct {
	rsPeer *rsPeer
}

func (e reScheduleParentEvent) apply(s *state) {
	rsPeer := e.rsPeer
	rsPeer.times = rsPeer.times + 1
	peer := rsPeer.peer
	if peer.Task.IsFail() {
		if err := peer.CloseChannelWithError(dferrors.New(base.Code_SchedTaskStatusError, "schedule task status failed")); err != nil {
			logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Warnf("close peer channel failed: %v", err)
		}
		return
	}
	oldParent, ok := peer.GetParent()
	blocklist := rsPeer.blocklist
	if ok && !blocklist.Contains(oldParent.ID) {
		logger.WithTaskAndPeerID(peer.Task.ID,
			peer.ID).Warnf("reScheduleParent： peer already schedule a parent %s and new parent is not in blank parents", oldParent.ID)
		return
	}

	parent, candidates, hasParent := s.sched.ScheduleParent(peer, blocklist)
	if !hasParent {
		if peer.Task.CanBackToSource() && !peer.Task.ContainsBackToSourcePeer(peer.ID) {
			if peer.CloseChannelWithError(dferrors.Newf(base.Code_SchedNeedBackSource, "peer %s need back source", peer.ID)) == nil {
				peer.Task.AddBackToSourcePeer(peer.ID)
			}
			return
		}
		logger.Errorf("reScheduleParent: failed to schedule parent to peer %s, reschedule it later", peer.ID)
		s.waitScheduleParentPeerQueue.AddAfter(rsPeer, time.Second)
		return
	}

	// TODO if parentPeer is equal with oldParent, need schedule again ?
	if err := peer.SendSchedulePacket(constructSuccessPeerPacket(peer, parent, candidates)); err != nil {
		sendErrorHandler(err, s, peer)
	}
}

func (e reScheduleParentEvent) hashKey() string {
	return e.rsPeer.peer.Task.ID
}

type startReportPieceResultEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
}

func (e startReportPieceResultEvent) apply(s *state) {
	span := trace.SpanFromContext(e.ctx)
	if parent, ok := e.peer.GetParent(); ok {
		e.peer.Log().Warnf("startReportPieceResultEvent: no need schedule parent because peer already had parent %s", parent.ID)
		if err := e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parent, nil)); err != nil {
			sendErrorHandler(err, s, e.peer)
		}
		return
	}
	if e.peer.Task.ContainsBackToSourcePeer(e.peer.ID) {
		e.peer.Log().Info("startReportPieceResultEvent: no need schedule parent because peer is back source peer")
		return
	}

	parent, candidates, hasParent := s.sched.ScheduleParent(e.peer, set.NewSafeSet())
	// No parent node is currently available
	if !hasParent {
		if e.peer.Task.CanBackToSource() && !e.peer.Task.ContainsBackToSourcePeer(e.peer.ID) {
			span.SetAttributes(config.AttributeClientBackSource.Bool(true))
			if e.peer.CloseChannelWithError(dferrors.Newf(base.Code_SchedNeedBackSource, "peer %s need back source", e.peer.ID)) == nil {
				e.peer.Task.AddBackToSourcePeer(e.peer.ID)
			}
			logger.WithTaskAndPeerID(e.peer.Task.ID,
				e.peer.ID).Info("startReportPieceResultEvent: peer need back source because no parent node is available for scheduling")
			return
		}
		e.peer.Log().Warnf("startReportPieceResultEvent: no parent node is currently available，reschedule it later")
		s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: e.peer}, time.Second)
		return
	}
	if err := e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parent, candidates)); err != nil {
		sendErrorHandler(err, s, e.peer)
	}
}

func (e startReportPieceResultEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadPieceSuccessEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
	pr   *rpcscheduler.PieceResult
}

func (e peerDownloadPieceSuccessEvent) apply(s *state) {
	e.peer.AddPiece(e.pr.FinishedCount, int(e.pr.EndTime-e.pr.BeginTime))
	if e.peer.Task.ContainsBackToSourcePeer(e.peer.ID) {
		e.peer.Task.GetOrAddPiece(e.pr.PieceInfo)
		if !e.peer.Task.IsHealth() {
			e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: update task status seeding")
			e.peer.Task.SetStatus(supervisor.TaskStatusRunning)
		}
		return
	}

	var candidates []*supervisor.Peer
	parentPeer, ok := s.peerManager.Get(e.pr.DstPid)
	if !ok {
		e.peer.Log().Warnf("parent peer %s not found", e.pr.DstPid)
		return
	}

	if parentPeer.IsLeave() {
		e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: need reschedule parent for peer because it's parent is already left")
		e.peer.DeleteParent()
		var hasParent bool
		blocklist := set.NewSafeSet()
		blocklist.Add(parentPeer.ID)
		parentPeer, candidates, hasParent = s.sched.ScheduleParent(e.peer, blocklist)
		if !hasParent {
			e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: no parent node is currently available, " +
				"reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: e.peer, blocklist: blocklist}, time.Second)
			return
		}
	}

	if oldParent, ok := e.peer.GetParent(); e.pr.DstPid != e.peer.ID && (!ok || oldParent.ID != e.pr.DstPid) {
		logger.WithTaskAndPeerID(e.peer.Task.ID, e.peer.ID).Debugf("parent peerID is not same as DestPid, replace it's parent node with %s",
			e.pr.DstPid)
		e.peer.ReplaceParent(parentPeer)
	}

	parentPeer.LastAccessAt.Store(time.Now())
	if parentPeer.ID == e.pr.DstPid {
		return
	}

	// TODO if parentPeer is equal with oldParent, need schedule again ?
	if err := e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parentPeer, candidates)); err != nil {
		sendErrorHandler(err, s, e.peer)
	}
}

func (e peerDownloadPieceSuccessEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadPieceFailEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
	pr   *rpcscheduler.PieceResult
}

func (e peerDownloadPieceFailEvent) apply(s *state) {
	if e.peer.Task.ContainsBackToSourcePeer(e.peer.ID) {
		return
	}
	switch e.pr.Code {
	case base.Code_ClientWaitPieceReady:
		return
	case base.Code_PeerTaskNotFound:
		s.peerManager.Delete(e.pr.DstPid)
	case base.Code_CDNTaskNotFound, base.Code_CDNError, base.Code_CDNTaskDownloadFail:
		s.peerManager.Delete(e.pr.DstPid)
		go func() {
			if _, _, err := s.cdn.TriggerTask(e.ctx, e.peer.Task); err != nil {
				e.peer.Log().Errorf("peerDownloadPieceFailEvent: seed task failed: %v", err)
			}
		}()
	default:
		e.peer.Log().Debugf("report piece download fail message, piece result %s", e.pr.String())
	}
	blocklist := set.NewSafeSet()
	blocklist.Add(e.pr.DstPid)
	s.waitScheduleParentPeerQueue.Add(&rsPeer{peer: e.peer, blocklist: blocklist})
}
func (e peerDownloadPieceFailEvent) hashKey() string {
	return e.peer.Task.ID
}

type taskSeedFailEvent struct {
	task *supervisor.Task
}

func (e taskSeedFailEvent) apply(s *state) {
	handleCDNSeedTaskFail(e.task)
}

func (e taskSeedFailEvent) hashKey() string {
	return e.task.ID
}

type peerDownloadSuccessEvent struct {
	peer       *supervisor.Peer
	peerResult *rpcscheduler.PeerResult
}

func (e peerDownloadSuccessEvent) apply(s *state) {
	peer := e.peer
	task := e.peer.Task
	result := e.peerResult

	// If the cdn backsource successfully or the peer backsource successfully, the task status is successful.
	peer.SetStatus(supervisor.PeerStatusSuccess)
	if (peer.Task.ContainsBackToSourcePeer(e.peer.ID) || peer.Host.IsCDN) && !peer.Task.IsSuccess() {
		task.SetStatus(supervisor.TaskStatusSuccess)
		task.TotalPieceCount.Store(result.TotalPieceCount)
		task.ContentLength.Store(result.ContentLength)
	}

	removePeerFromCurrentTree(e.peer, s)
	if children, ok := s.sched.ScheduleChildren(e.peer, set.NewSafeSet()); ok {
		for _, child := range children {
			if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, e.peer, nil)); err != nil {
				sendErrorHandler(err, s, child)
			}
		}
	}
}

func (e peerDownloadSuccessEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadFailEvent struct {
	peer       *supervisor.Peer
	peerResult *rpcscheduler.PeerResult
}

func (e peerDownloadFailEvent) apply(s *state) {
	e.peer.SetStatus(supervisor.PeerStatusFail)
	if e.peer.Task.ContainsBackToSourcePeer(e.peer.ID) && !e.peer.Task.IsSuccess() {
		e.peer.Task.SetStatus(supervisor.TaskStatusFail)
		handleCDNSeedTaskFail(e.peer.Task)
		return
	}
	removePeerFromCurrentTree(e.peer, s)
	e.peer.GetChildren().Range(func(key, value interface{}) bool {
		child := (value).(*supervisor.Peer)

		blocklist := set.NewSafeSet()
		blocklist.Add(e.peer.ID)
		parent, candidates, hasParent := s.sched.ScheduleParent(child, blocklist)
		if !hasParent {
			e.peer.Log().Warnf("peerDownloadFailEvent: there is no available parent, reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: e.peer, blocklist: blocklist}, time.Second)
			return true
		}
		if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidates)); err != nil {
			sendErrorHandler(err, s, child)
		}
		return true
	})
}

func (e peerDownloadFailEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerLeaveEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
}

var _ event = peerLeaveEvent{}

func (e peerLeaveEvent) apply(s *state) {
	e.peer.Leave()
	removePeerFromCurrentTree(e.peer, s)
	e.peer.GetChildren().Range(func(key, value interface{}) bool {
		child := value.(*supervisor.Peer)

		blocklist := set.NewSafeSet()
		blocklist.Add(e.peer.ID)
		parent, candidateParents, ok := s.sched.ScheduleParent(child, blocklist)
		if !ok {
			e.peer.Log().Warnf("handlePeerLeave: there is no available parent，reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: child, blocklist: blocklist}, time.Second)
			return true
		}
		if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidateParents)); err != nil {
			sendErrorHandler(err, s, child)
		}
		return true
	})
	s.peerManager.Delete(e.peer.ID)
}

func (e peerLeaveEvent) hashKey() string {
	return e.peer.Task.ID
}

// constructSuccessPeerPacket construct success schedule packet
func constructSuccessPeerPacket(peer *supervisor.Peer, parent *supervisor.Peer, candidateParents []*supervisor.Peer) *rpcscheduler.PeerPacket {
	var stealPeers []*rpcscheduler.PeerPacket_DestPeer
	for _, candidateParent := range candidateParents {
		stealPeers = append(stealPeers, &rpcscheduler.PeerPacket_DestPeer{
			Ip:      candidateParent.Host.IP,
			RpcPort: candidateParent.Host.RPCPort,
			PeerId:  candidateParent.ID,
		})
	}

	// TODO(gaius-qi) Configure ParallelCount in the manager
	return &rpcscheduler.PeerPacket{
		TaskId:        peer.Task.ID,
		SrcPid:        peer.ID,
		ParallelCount: 1,
		MainPeer: &rpcscheduler.PeerPacket_DestPeer{
			Ip:      parent.Host.IP,
			RpcPort: parent.Host.RPCPort,
			PeerId:  parent.ID,
		},
		StealPeers: stealPeers,
		Code:       base.Code_Success,
	}
}

func handleCDNSeedTaskFail(task *supervisor.Task) {
	if !task.CanBackToSource() {
		task.SetStatus(supervisor.TaskStatusFail)
	}

	task.GetPeers().Range(func(item interface{}) bool {
		peer, ok := item.(*supervisor.Peer)
		if !ok {
			return true
		}

		// If the task status failed and can backsource, notify dfdaemon to backsource
		if task.CanBackToSource() {
			if !task.ContainsBackToSourcePeer(peer.ID) {
				if err := peer.CloseChannelWithError(dferrors.Newf(base.Code_SchedNeedBackSource, "peer %s need back source because cdn seed task failed", peer.ID)); err != nil {
					peer.Log().Errorf("close channel with backsource failed: %v", err)
				}
				task.AddBackToSourcePeer(peer.ID)
				peer.Log().Info("notify dfdaemon to backsource successfully")
				return true
			}

			peer.Log().Info("has contains backsource peer list")
			return true
		}

		// If the task status failed, notify dfdaemon to task error
		if err := peer.CloseChannelWithError(dferrors.New(base.Code_SchedTaskStatusError, "cdn seed task failed")); err != nil {
			peer.SetStatus(supervisor.PeerStatusFail)
			peer.Log().Warnf("close channel failed: %v", err)
			return true
		}

		return true
	})
}

func removePeerFromCurrentTree(peer *supervisor.Peer, s *state) {
	parent, ok := peer.GetParent()
	if !ok {
		return
	}
	peer.DeleteParent()

	blocklist := set.NewSafeSet()
	blocklist.Add(peer.ID)
	if children, ok := s.sched.ScheduleChildren(parent, blocklist); ok {
		for _, child := range children {
			if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, peer, nil)); err != nil {
				sendErrorHandler(err, s, child)
			}
		}
	}
}

func sendErrorHandler(err error, s *state, p *supervisor.Peer) {
	if err == supervisor.ErrChannelBusy {
		p.Log().Info("send schedule packet channel busy")
		s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: p}, 10*time.Millisecond)
	} else {
		p.Log().Errorf("send schedule packet failed: %v", err)
	}
}
