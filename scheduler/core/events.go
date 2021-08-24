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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/util/workqueue"
)

type event interface {
	hashKey() string
	apply(s *state)
}

type state struct {
	sched                       scheduler.Scheduler
	peerManager                 supervisor.PeerMgr
	cdnManager                  supervisor.CDNMgr
	waitScheduleParentPeerQueue workqueue.DelayingInterface
}

func newState(sched scheduler.Scheduler, peerManager supervisor.PeerMgr, cdnManager supervisor.CDNMgr, wsdq workqueue.DelayingInterface) *state {
	return &state{
		sched:                       sched,
		peerManager:                 peerManager,
		cdnManager:                  cdnManager,
		waitScheduleParentPeerQueue: wsdq,
	}
}

type reScheduleParentEvent struct {
	peer *supervisor.Peer
}

var _ event = reScheduleParentEvent{}

func (e reScheduleParentEvent) apply(s *state) {
	reScheduleParent(e.peer, s)
}

func (e reScheduleParentEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type startReportPieceResultEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
}

var _ event = startReportPieceResultEvent{}

func (e startReportPieceResultEvent) apply(s *state) {
	span := trace.SpanFromContext(e.ctx)
	if e.peer.GetParent() != nil {
		logger.WithTaskAndPeerID(e.peer.Task.TaskID,
			e.peer.PeerID).Warnf("startReportPieceResultEvent: no need schedule parent because peer already had parent %s", e.peer.GetParent().PeerID)
		return
	}
	if e.peer.Task.IsWaitingClientBackSource() {
		e.peer.Task.IncreaseBackSourceClientCount()
		e.peer.SendSchedulePacket(constructFailPeerPacket(e.peer, dfcodes.SchedNeedBackSource))
	}
	parent, candidates, hasParent := s.sched.ScheduleParent(e.peer)
	span.AddEvent("parent")
	if !hasParent {
		logger.WithTaskAndPeerID(e.peer.Task.TaskID, e.peer.PeerID).Warnf("startReportPieceResultEvent: there is no available parent，reschedule it in one second")
		s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
		return
	}
	e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parent, candidates))
}

func (e startReportPieceResultEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type peerDownloadPieceSuccessEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceSuccessEvent{}

func (e peerDownloadPieceSuccessEvent) apply(s *state) {
	span := trace.SpanFromContext(e.ctx)
	span.AddEvent("piece success")
	e.peer.UpdateProgress(e.pr.FinishedCount, int(e.pr.EndTime-e.pr.BeginTime))
	if e.peer.Task.IsWaitingClientBackSource() {
		e.peer.Task.SetStatus(supervisor.TaskStatusSeeding)
	}
	e.peer.Task.AddPiece(e.pr.PieceInfo)
	oldParent := e.peer.GetParent()
	var candidates []*supervisor.Peer
	parentPeer, ok := s.peerManager.Get(e.pr.DstPid)
	if !ok || parentPeer.IsLeave() {
		e.peer.ReplaceParent(nil)
		var hasParent bool
		parentPeer, candidates, hasParent = s.sched.ScheduleParent(e.peer)
		if !hasParent {
			logger.WithTaskAndPeerID(e.peer.Task.TaskID, e.peer.PeerID).Warnf("peerDownloadPieceSuccessEvent: there is no available parent, reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
			return
		}
	}
	parentPeer.Touch()
	if oldParent != nil {
		candidates = append(candidates, oldParent)
	}
	// TODO if parentPeer is equal with oldParent, need schedule again ?
	e.peer.SendSchedulePacket(constructSuccessPeerPacket(e.peer, parentPeer, candidates))
	return
}

func (e peerDownloadPieceSuccessEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type peerDownloadPieceFailEvent struct {
	peer *supervisor.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceFailEvent{}

func (e peerDownloadPieceFailEvent) apply(s *state) {
	switch e.pr.Code {
	case dfcodes.PeerTaskNotFound, dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		// TODO PeerTaskNotFound remove dest peer task, ClientPieceDownloadFail add blank list
		reScheduleParent(e.peer, s)
		return
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail, dfcodes.CdnTaskDownloadFail:
		go func(task *supervisor.Task) {
			if e.peer.Task.IsHealth() {
				return
			}
			// TODO
			synclock.Lock(task.TaskID, false)
			defer synclock.UnLock(task.TaskID, false)
			task.SetStatus(supervisor.TaskStatusRunning)
			if cdnPeer, err := s.cdnManager.StartSeedTask(context.Background(), task); err != nil {
				logger.Errorf("start seed task fail: %v", err)
				task.SetStatus(supervisor.TaskStatusWaitingClientBackSource)

				handleSeedTaskFail(task)
			} else {
				logger.Debugf("===== successfully obtain seeds from cdn, task: %+v =====", e.peer.Task)
				children := s.sched.ScheduleChildren(cdnPeer)
				for _, child := range children {
					child.SendSchedulePacket(constructSuccessPeerPacket(child, cdnPeer, nil))
				}
			}
		}(e.peer.Task)
	default:
		reScheduleParent(e.peer, s)
		return
	}
}
func (e peerDownloadPieceFailEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type taskSeedFailEvent struct {
	task *supervisor.Task
}

var _ event = taskSeedFailEvent{}

func (e taskSeedFailEvent) apply(s *state) {
	handleSeedTaskFail(e.task)
}

func (e taskSeedFailEvent) hashKey() string {
	return e.task.TaskID
}

type peerDownloadSuccessEvent struct {
	peer       *supervisor.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadSuccessEvent{}

func (e peerDownloadSuccessEvent) apply(s *state) {
	e.peer.SetStatus(supervisor.PeerStatusSuccess)
	e.peer.Task.UpdateTaskSuccessResult(e.peerResult.TotalPieceCount, e.peerResult.ContentLength)
	removePeerFromCurrentTree(e.peer, s)
	children := s.sched.ScheduleChildren(e.peer)
	for _, child := range children {
		child.SendSchedulePacket(constructSuccessPeerPacket(child, e.peer, nil))
	}
	e.peer.UnBindSendChannel()
}

func (e peerDownloadSuccessEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type peerDownloadFailEvent struct {
	peer       *supervisor.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadFailEvent{}

func (e peerDownloadFailEvent) apply(s *state) {
	e.peer.SetStatus(supervisor.PeerStatusFail)
	removePeerFromCurrentTree(e.peer, s)
	e.peer.GetChildren().Range(func(key, value interface{}) bool {
		child := (value).(*supervisor.Peer)
		parent, candidates, hasParent := s.sched.ScheduleParent(child)
		if !hasParent {
			logger.WithTaskAndPeerID(child.Task.TaskID, child.PeerID).Warnf("peerDownloadFailEvent: there is no available parent, reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
			return true
		}
		child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidates))
		return true
	})
}

func (e peerDownloadFailEvent) hashKey() string {
	return e.peer.Task.TaskID
}

type peerLeaveEvent struct {
	ctx  context.Context
	peer *supervisor.Peer
}

var _ event = peerLeaveEvent{}

func (e peerLeaveEvent) apply(s *state) {
	e.peer.MarkLeave()
	removePeerFromCurrentTree(e.peer, s)
	e.peer.GetChildren().Range(func(key, value interface{}) bool {
		child := value.(*supervisor.Peer)
		parent, candidates, hasParent := s.sched.ScheduleParent(child)
		if !hasParent {
			logger.WithTaskAndPeerID(child.Task.TaskID, child.PeerID).Warnf("handlePeerLeave: there is no available parent，reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(child, time.Second)
			return true
		}
		child.SendSchedulePacket(constructSuccessPeerPacket(child, parent, candidates))
		return true
	})
	s.peerManager.Delete(e.peer.PeerID)
}

func (e peerLeaveEvent) hashKey() string {
	return e.peer.Task.TaskID
}

func constructSuccessPeerPacket(peer *supervisor.Peer, parent *supervisor.Peer, candidates []*supervisor.Peer) *schedulerRPC.PeerPacket {
	mainPeer := &schedulerRPC.PeerPacket_DestPeer{
		Ip:      parent.Host.IP,
		RpcPort: parent.Host.RPCPort,
		PeerId:  parent.PeerID,
	}
	var stealPeers []*schedulerRPC.PeerPacket_DestPeer
	for _, candidate := range candidates {
		stealPeers = append(stealPeers, &schedulerRPC.PeerPacket_DestPeer{
			Ip:      candidate.Host.IP,
			RpcPort: candidate.Host.RPCPort,
			PeerId:  candidate.PeerID,
		})
	}
	peerPacket := &schedulerRPC.PeerPacket{
		TaskId:        peer.Task.TaskID,
		SrcPid:        peer.PeerID,
		ParallelCount: 1,
		MainPeer:      mainPeer,
		StealPeers:    stealPeers,
		Code:          dfcodes.Success,
	}
	logger.Debugf("send peerPacket %+v to peer %s", peerPacket, peer.PeerID)
	return peerPacket
}

func constructFailPeerPacket(peer *supervisor.Peer, errCode base.Code) *schedulerRPC.PeerPacket {
	return &schedulerRPC.PeerPacket{
		TaskId: peer.Task.TaskID,
		SrcPid: peer.PeerID,
		Code:   errCode,
	}
}

func reScheduleParent(peer *supervisor.Peer, s *state) {
	parent, candidates, hasParent := s.sched.ScheduleParent(peer)
	if !hasParent {
		logger.Errorf("handleReplaceParent: failed to schedule parent to peer %s, reschedule it in one second", peer.PeerID)
		//peer.PacketChan <- constructFailPeerPacket(peer, dfcodes.SchedWithoutParentPeer)
		s.waitScheduleParentPeerQueue.AddAfter(peer, time.Second)
		return
	}
	peer.SendSchedulePacket(constructSuccessPeerPacket(peer, parent, candidates))
}

func handleSeedTaskFail(task *supervisor.Task) {
	if task.IsFail() {
		task.ListPeers().Range(func(data sortedlist.Item) bool {
			peer := data.(*supervisor.Peer)
			peer.SendSchedulePacket(constructFailPeerPacket(peer, dfcodes.SchedNeedBackSource))
			return true
		})
	}
}

func removePeerFromCurrentTree(peer *supervisor.Peer, s *state) {
	parent := peer.GetParent()
	peer.ReplaceParent(nil)
	// parent frees up upload resources
	if parent != nil {
		children := s.sched.ScheduleChildren(parent)
		for _, child := range children {
			child.SendSchedulePacket(constructSuccessPeerPacket(child, peer, nil))
		}
	}
}
