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

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
)

type event interface {
	hashKey() string
	apply(s *state)
}

type rsPeer struct {
	times        int32
	peer         *supervisor.Peer
	blankParents sets.String
}

type state struct {
	sched                       scheduler.Scheduler
	peerManager                 supervisor.PeerManager
	cdn                         supervisor.CDN
	waitScheduleParentPeerQueue workqueue.DelayingInterface
}

func newState(sched scheduler.Scheduler, peerManager supervisor.PeerManager, cdn supervisor.CDN, wsdq workqueue.DelayingInterface) *state {
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

var _ event = reScheduleParentEvent{}

func (e reScheduleParentEvent) apply(s *state) {
	rsPeer := e.rsPeer
	rsPeer.times = rsPeer.times + 1
	peer := rsPeer.peer
	if peer.Task.IsFail() {
		if err := peer.CloseChannel(dferrors.New(dfcodes.SchedTaskStatusError, "schedule task status failed")); err != nil {
			logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Warnf("close peer channel failed: %v", err)
		}
		return
	}
	oldParent, ok := peer.GetParent()
	blankParents := rsPeer.blankParents
	if ok && !blankParents.Has(oldParent.ID) {
		logger.WithTaskAndPeerID(peer.Task.ID,
			peer.ID).Warnf("reScheduleParent： peer already schedule a parent %s and new parent is not in blank parents", oldParent.ID)
		return
	}

	parent, candidates, hasParent := s.sched.ScheduleParent(peer, blankParents)
	if !hasParent {
		if peer.Task.NeedClientBackSource() && !peer.Task.ContainsBackSourcePeer(peer.ID) {
			if peer.CloseChannel(dferrors.Newf(dfcodes.SchedNeedBackSource, "peer %s need back source", peer.ID)) == nil {
				peer.Task.AddBackSourcePeer(peer.ID)
			}
			return
		}
		logger.Errorf("reScheduleParent: failed to schedule parent to peer %s, reschedule it later", peer.ID)
		s.waitScheduleParentPeerQueue.AddAfter(rsPeer, time.Second)
		return
	}

	// TODO if parentPeer is equal with oldParent, need schedule again ?
	if err := peer.SendSchedulePacket(constructSuccessPeerPacket(peer, parent, candidates)); err != nil {
		logger.WithTaskAndPeerID(peer.Task.ID, peer.ID).Warnf("send schedule packet to peer %s failed: %v", peer.ID, err)
	}
}

func (e reScheduleParentEvent) hashKey() string {
	return e.rsPeer.peer.Task.ID
}

type startReportPieceResultEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
}

var _ event = startReportPieceResultEvent{}

func (e startReportPieceResultEvent) apply(s *state) {
	span := trace.SpanFromContext(e.ctx)
	if parent, ok := e.peer.GetParent(); ok {
		e.peer.Log().Warnf("startReportPieceResultEvent: no need schedule parent because peer already had parent %s", parent.ID)
		if err := e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parent, nil)); err != nil {
			logger.WithTaskAndPeerID(e.peer.Task.ID, e.peer.ID).Warnf("send schedule packet to peer failed: %v", err)
		}
		return
	}
	if e.peer.Task.ContainsBackSourcePeer(e.peer.ID) {
		e.peer.Log().Info("startReportPieceResultEvent: no need schedule parent because peer is back source peer")
		return
	}

	parent, candidates, hasParent := s.sched.ScheduleParent(e.peer, sets.NewString())
	// No parent node is currently available
	if !hasParent {
		if e.peer.Task.NeedClientBackSource() && !e.peer.Task.ContainsBackSourcePeer(e.peer.ID) {
			span.SetAttributes(config.AttributeClientBackSource.Bool(true))
			if e.peer.CloseChannel(dferrors.Newf(dfcodes.SchedNeedBackSource, "peer %s need back source", e.peer.ID)) == nil {
				e.peer.Task.AddBackSourcePeer(e.peer.ID)
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
		e.peer.Log().Warnf("send schedule packet failed: %v", err)
	}
}

func (e startReportPieceResultEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadPieceSuccessEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceSuccessEvent{}

func (e peerDownloadPieceSuccessEvent) apply(s *state) {
	e.peer.UpdateProgress(e.pr.FinishedCount, int(e.pr.EndTime-e.pr.BeginTime))
	if e.peer.Task.ContainsBackSourcePeer(e.peer.ID) {
		e.peer.Task.GetOrAddPiece(e.pr.PieceInfo)
		if !e.peer.Task.CanSchedule() {
			e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: update task status seeding")
			e.peer.Task.SetStatus(supervisor.TaskStatusSeeding)
		}
		return
	}
	var candidates []*supervisor.Peer
	parentPeer, ok := s.peerManager.Get(e.pr.DstPid)
	if ok {
		if oldParent, ok := e.peer.GetParent(); e.pr.DstPid != e.peer.ID && (!ok || oldParent.ID != e.pr.DstPid) {
			logger.WithTaskAndPeerID(e.peer.Task.ID, e.peer.ID).Debugf("parent peerID is not same as DestPid, replace it's parent node with %s",
				e.pr.DstPid)
			e.peer.ReplaceParent(parentPeer)
		}
	} else if parentPeer.IsLeave() {
		e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: need reschedule parent for peer because it's parent is already left")
		e.peer.ReplaceParent(nil)
		var hasParent bool
		parentPeer, candidates, hasParent = s.sched.ScheduleParent(e.peer, sets.NewString(parentPeer.ID))
		if !hasParent {
			e.peer.Log().Warnf("peerDownloadPieceSuccessEvent: no parent node is currently available, " +
				"reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: e.peer, blankParents: sets.NewString(parentPeer.ID)}, time.Second)
			return
		}
	}
	parentPeer.Touch()
	if parentPeer.ID == e.pr.DstPid {
		return
	}
	// TODO if parentPeer is equal with oldParent, need schedule again ?
	if err := e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parentPeer, candidates)); err != nil {
		e.peer.Log().Warnf("send schedule packet to peer %s failed: %v", e.peer.ID, err)
	}
}

func (e peerDownloadPieceSuccessEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadPieceFailEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceFailEvent{}

func (e peerDownloadPieceFailEvent) apply(s *state) {
	if e.peer.Task.ContainsBackSourcePeer(e.peer.ID) {
		return
	}
	switch e.pr.Code {
	case dfcodes.ClientWaitPieceReady:
		return
	case dfcodes.PeerTaskNotFound:
		s.peerManager.Delete(e.pr.DstPid)
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskDownloadFail:
		s.peerManager.Delete(e.pr.DstPid)
		go func() {
			if _, err := s.cdn.StartSeedTask(e.ctx, e.peer.Task); err != nil {
				e.peer.Log().Errorf("peerDownloadPieceFailEvent: seed task failed: %v", err)
			}
		}()
	default:
		e.peer.Log().Debugf("report piece download fail message, piece result %s", e.pr.String())
	}
	s.waitScheduleParentPeerQueue.Add(&rsPeer{peer: e.peer, blankParents: sets.NewString(e.pr.DstPid)})
}
func (e peerDownloadPieceFailEvent) hashKey() string {
	return e.peer.Task.ID
}

type taskSeedFailEvent struct {
	task *supervisor.Task
}

var _ event = taskSeedFailEvent{}

func (e taskSeedFailEvent) apply(s *state) {
	handleCDNSeedTaskFail(e.task)
}

func (e taskSeedFailEvent) hashKey() string {
	return e.task.ID
}

type peerDownloadSuccessEvent struct {
	peer       *supervisor.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadSuccessEvent{}

func (e peerDownloadSuccessEvent) apply(s *state) {
	e.peer.SetStatus(supervisor.PeerStatusSuccess)
	if e.peer.Task.ContainsBackSourcePeer(e.peer.ID) && !e.peer.Task.IsSuccess() {
		e.peer.Task.UpdateTaskSuccessResult(e.peerResult.TotalPieceCount, e.peerResult.ContentLength)
	}
	removePeerFromCurrentTree(e.peer, s)
	children := s.sched.ScheduleChildren(e.peer, sets.NewString())
	for _, child := range children {
		if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, e.peer, nil)); err != nil {
			e.peer.Log().Warnf("send schedule packet to peer %s failed: %v", child.ID, err)
		}
	}
}

func (e peerDownloadSuccessEvent) hashKey() string {
	return e.peer.Task.ID
}

type peerDownloadFailEvent struct {
	peer       *supervisor.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadFailEvent{}

func (e peerDownloadFailEvent) apply(s *state) {
	e.peer.SetStatus(supervisor.PeerStatusFail)
	if e.peer.Task.ContainsBackSourcePeer(e.peer.ID) && !e.peer.Task.IsSuccess() {
		e.peer.Task.SetStatus(supervisor.TaskStatusFail)
		handleCDNSeedTaskFail(e.peer.Task)
		return
	}
	removePeerFromCurrentTree(e.peer, s)
	e.peer.GetChildren().Range(func(key, value interface{}) bool {
		child := (value).(*supervisor.Peer)
		parent, candidates, hasParent := s.sched.ScheduleParent(child, sets.NewString(e.peer.ID))
		if !hasParent {
			e.peer.Log().Warnf("peerDownloadFailEvent: there is no available parent, reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: e.peer, blankParents: sets.NewString(e.peer.ID)}, time.Second)
			return true
		}
		if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidates)); err != nil {
			e.peer.Log().Warnf("send schedule packet to peer %s failed: %v", child.ID, err)
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
		parent, candidates, hasParent := s.sched.ScheduleParent(child, sets.NewString(e.peer.ID))
		if !hasParent {
			e.peer.Log().Warnf("handlePeerLeave: there is no available parent，reschedule it later")
			s.waitScheduleParentPeerQueue.AddAfter(&rsPeer{peer: child, blankParents: sets.NewString(e.peer.ID)}, time.Second)
			return true
		}
		if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidates)); err != nil {
			e.peer.Log().Warnf("send schedule packet to peer %s failed: %v", child.ID, err)
		}
		return true
	})
	s.peerManager.Delete(e.peer.ID)
}

func (e peerLeaveEvent) hashKey() string {
	return e.peer.Task.ID
}

// constructSuccessPeerPacket construct success peer schedule packet
func constructSuccessPeerPacket(peer *supervisor.Peer, parent *supervisor.Peer, candidates []*supervisor.Peer) *schedulerRPC.PeerPacket {
	mainPeer := &schedulerRPC.PeerPacket_DestPeer{
		Ip:      parent.Host.IP,
		RpcPort: parent.Host.RPCPort,
		PeerId:  parent.ID,
	}
	var stealPeers []*schedulerRPC.PeerPacket_DestPeer
	for _, candidate := range candidates {
		stealPeers = append(stealPeers, &schedulerRPC.PeerPacket_DestPeer{
			Ip:      candidate.Host.IP,
			RpcPort: candidate.Host.RPCPort,
			PeerId:  candidate.ID,
		})
	}
	peerPacket := &schedulerRPC.PeerPacket{
		TaskId:        peer.Task.ID,
		SrcPid:        peer.ID,
		ParallelCount: 1,
		MainPeer:      mainPeer,
		StealPeers:    stealPeers,
		Code:          dfcodes.Success,
	}
	logger.Debugf("send peerPacket %+v to peer %s", peerPacket, peer.ID)
	return peerPacket
}

func handleCDNSeedTaskFail(task *supervisor.Task) {
	if task.NeedClientBackSource() {
		task.GetPeers().Range(func(data sortedlist.Item) bool {
			peer := data.(*supervisor.Peer)
			if task.NeedClientBackSource() {
				if !task.ContainsBackSourcePeer(peer.ID) {
					if peer.CloseChannel(dferrors.Newf(dfcodes.SchedNeedBackSource, "peer %s need back source because cdn seed task failed", peer.ID)) == nil {
						task.AddBackSourcePeer(peer.ID)
					}
				}
				return true
			}
			return false
		})
	} else {
		task.GetPeers().Range(func(data sortedlist.Item) bool {
			peer := data.(*supervisor.Peer)
			if err := peer.CloseChannel(dferrors.New(dfcodes.SchedTaskStatusError, "schedule task status failed")); err != nil {
				peer.Log().Warnf("close peer conn channel failed: %v", err)
			}
			return true
		})
	}
}

func removePeerFromCurrentTree(peer *supervisor.Peer, s *state) {
	parent, ok := peer.GetParent()
	peer.ReplaceParent(nil)
	// parent frees up upload resources
	if ok {
		children := s.sched.ScheduleChildren(parent, sets.NewString(peer.ID))
		for _, child := range children {
			if err := child.SendSchedulePacket(constructSuccessPeerPacket(child, peer, nil)); err != nil {
				peer.Log().Warnf("send schedule packet to peer %s failed: %v", child.ID, err)
			}
		}
	}
}
