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
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
	"k8s.io/client-go/util/workqueue"
)

type event interface {
	hashKey() string
	apply(s *state)
}

type state struct {
	sched                         scheduler.Scheduler
	peerManager                   daemon.PeerMgr
	cdnManager                    daemon.CDNMgr
	waitScheduleParentPeerQueue   workqueue.DelayingInterface
	waitScheduleChildrenPeerQueue workqueue.DelayingInterface
}

func newState(sched scheduler.Scheduler, peerManager daemon.PeerMgr, cdnManager daemon.CDNMgr) *state {
	return &state{
		sched:                         sched,
		peerManager:                   peerManager,
		cdnManager:                    cdnManager,
		waitScheduleParentPeerQueue:   workqueue.NewNamedDelayingQueue("wait reSchedule parent"),
		waitScheduleChildrenPeerQueue: workqueue.NewNamedDelayingQueue("wait reSchedule children"),
	}
}

func (s *state) start() {
	go func() {
		for {
			v, shutdown := s.waitScheduleParentPeerQueue.Get()
			if shutdown {
				break
			}
			peer := v.(*types.Peer)
			parent, candidates, hashParent := s.sched.ScheduleParent(peer)
			if !hashParent {
				logger.WithTaskAndPeerID(peer.Task.TaskID, peer.PeerID).Warnf("waitScheduleParentPeerQueue: there is no available parent，reschedule it in one second")
				s.waitScheduleChildrenPeerQueue.Get()
				s.waitScheduleParentPeerQueue.AddAfter(peer, time.Second)
				continue
			}
			if peer.PacketChan == nil {
				logger.Errorf("waitScheduleParentPeerQueue: there is no packet chan associated with peer %s", peer.PeerID)
				return
			}
			peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
			s.waitScheduleParentPeerQueue.Done(v)
		}
	}()
	for {
		v, shutdown := s.waitScheduleChildrenPeerQueue.Get()
		if shutdown {
			break
		}
		peer := v.(*types.Peer)
		children := s.sched.ScheduleChildren(peer)
		if children == nil || len(children) == 0 {
			s.waitScheduleChildrenPeerQueue.AddAfter(peer, time.Second)
			continue
		}
		for _, child := range children {
			if child.PacketChan == nil {
				logger.Debugf("reportPeerSuccessResult: there is no packet chan with peer %s", peer.PeerID)
				continue
			}
			child.PacketChan <- constructSuccessPeerPacket(child, peer, nil)
		}
		s.waitScheduleChildrenPeerQueue.Done(v)
	}
}

type startReportPieceResultEvent struct {
	peer *types.Peer
}

var _ event = startReportPieceResultEvent{}

func (e startReportPieceResultEvent) apply(s *state) {
	parent, candidates, hasParent := s.sched.ScheduleParent(e.peer)
	if !hasParent {
		logger.WithTaskAndPeerID(e.peer.Task.TaskID, e.peer.PeerID).Warnf("peerScheduleParentEvent: there is no available parent，reschedule it in one second")
		s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
		//e.peer.PacketChan <- constructFailPeerPacket(e.peer, dfcodes.SchedWithoutParentPeer)
		return
	}
	if e.peer.PacketChan == nil {
		logger.Errorf("report piece result: there is no packet chan associated with peer %s", e.peer.PeerID)
		return
	}
	e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parent, candidates)
}

func (e startReportPieceResultEvent) hashKey() string {
	return e.peer.PeerID
}

type peerDownloadPieceSuccessEvent struct {
	peer *types.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceSuccessEvent{}

func (e peerDownloadPieceSuccessEvent) apply(s *state) {
	e.peer.AddPieceInfo(e.pr.FinishedCount, int(e.pr.EndTime-e.pr.BeginTime))
	oldParent := e.peer.GetParent()
	var candidates []*types.Peer
	parentPeer, ok := s.peerManager.Get(e.pr.DstPid)
	if !ok {
		var hasParent bool
		parentPeer, candidates, hasParent = s.sched.ScheduleParent(e.peer)
		if !hasParent {
			logger.WithTaskAndPeerID(e.peer.Task.TaskID, e.peer.PeerID).Warnf("peerDownloadPieceSuccessEvent: there is no available parent，reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
			return
		}
	}
	parentPeer.Touch()
	if oldParent != nil {
		candidates = append(candidates, oldParent)
	}
	if e.peer.PacketChan == nil {
		logger.Errorf("peerDownloadPieceSuccessEvent: there is no packet chan with peer %s", e.peer.PeerID)
		return
	}
	e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parentPeer, candidates)
	return
}

func (e peerDownloadPieceSuccessEvent) hashKey() string {
	return e.peer.PeerID
}

type peerDownloadPieceFailEvent struct {
	peer *types.Peer
	pr   *schedulerRPC.PieceResult
}

var _ event = peerDownloadPieceFailEvent{}

func (e peerDownloadPieceFailEvent) apply(s *state) {
	switch e.pr.Code {
	case dfcodes.PeerTaskNotFound:
		handlePeerLeave(e.peer, s)
		return
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		handleReplaceParent(e.peer, s)
		return
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail, dfcodes.CdnTaskDownloadFail:
		if err := s.cdnManager.StartSeedTask(context.Background(), e.peer.Task); err != nil {
			logger.Errorf("start seed task fail: %v", err)
			e.peer.Task.SetStatus(types.TaskStatusFailed)
			handleSeedTaskFail(e.peer.Task)
			return
		}
		logger.Debugf("===== successfully obtain seeds from cdn, task: %+v =====", e.peer.Task)
	default:
		handleReplaceParent(e.peer, s)
		return
	}
}
func (e peerDownloadPieceFailEvent) hashKey() string {
	return e.peer.PeerID
}

type peerReplaceParentEvent struct {
	peer *types.Peer
}

func (e peerReplaceParentEvent) hashKey() string {
	return e.peer.PeerID
}

func (e peerReplaceParentEvent) apply(s *state) {
	handleReplaceParent(e.peer, s)
}

var _ event = peerReplaceParentEvent{}

type taskSeedFailEvent struct {
	task *types.Task
}

var _ event = taskSeedFailEvent{}

func (e taskSeedFailEvent) apply(s *state) {
	handleSeedTaskFail(e.task)
}

func (e taskSeedFailEvent) hashKey() string {
	return e.task.TaskID
}

type peerDownloadSuccessEvent struct {
	peer       *types.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadSuccessEvent{}

func (e peerDownloadSuccessEvent) apply(s *state) {
	e.peer.SetStatus(types.PeerStatusSuccess)
	children := s.sched.ScheduleChildren(e.peer)
	if children == nil || len(children) == 0 {
		logger.WithTaskAndPeerID(e.peer.Task.TaskID,
			e.peer.PeerID).Warnf("peerDownloadSuccessEvent: there is no available children，reschedule it in one second")
		s.waitScheduleChildrenPeerQueue.AddAfter(e.peer, time.Second)
		return
	}
	for _, child := range children {
		if child.PacketChan == nil {
			logger.Debugf("reportPeerSuccessResult: there is no packet chan with peer %s", e.peer.PeerID)
			continue
		}
		child.PacketChan <- constructSuccessPeerPacket(child, e.peer, nil)
	}
}

func (e peerDownloadSuccessEvent) hashKey() string {
	return e.peer.PeerID
}

type peerDownloadFailEvent struct {
	peer       *types.Peer
	peerResult *schedulerRPC.PeerResult
}

var _ event = peerDownloadFailEvent{}

func (e peerDownloadFailEvent) apply(s *state) {
	e.peer.SetStatus(types.PeerStatusFail)
	for _, child := range e.peer.GetChildren() {
		parent, candidates, hasParent := s.sched.ScheduleParent(child)
		if child.PacketChan == nil {
			logger.Warnf("reportPeerDownloadResult: there is no packet chan associated with peer %s", e.peer.PeerID)
			continue
		}
		if !hasParent {
			logger.WithTaskAndPeerID(child.Task.TaskID, child.PeerID).Warnf("peerDownloadFailEvent: there is no available parent，reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(e.peer, time.Second)
			return
		}
		child.PacketChan <- constructSuccessPeerPacket(child, parent, candidates)
	}
	s.peerManager.Delete(e.peer.PeerID)
}

func (e peerDownloadFailEvent) hashKey() string {
	return e.peer.PeerID
}

type peerLeaveEvent struct {
	peer *types.Peer
}

var _ event = peerLeaveEvent{}

func (e peerLeaveEvent) apply(s *state) {
	handlePeerLeave(e.peer, s)
}

func (e peerLeaveEvent) hashKey() string {
	return e.peer.PeerID
}

func constructSuccessPeerPacket(peer *types.Peer, parent *types.Peer, candidates []*types.Peer) *schedulerRPC.PeerPacket {
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
	return &schedulerRPC.PeerPacket{
		TaskId:        peer.Task.TaskID,
		SrcPid:        peer.PeerID,
		ParallelCount: 0,
		MainPeer:      mainPeer,
		StealPeers:    stealPeers,
		Code:          dfcodes.Success,
	}
}

func constructFailPeerPacket(peer *types.Peer, errCode base.Code) *schedulerRPC.PeerPacket {
	return &schedulerRPC.PeerPacket{
		TaskId: peer.Task.TaskID,
		SrcPid: peer.PeerID,
		Code:   errCode,
	}
}

func handlePeerLeave(peer *types.Peer, s *state) {
	peer.MarkLeave()
	peer.ReplaceParent(nil)
	for _, child := range peer.GetChildren() {
		parent, candidates, hasParent := s.sched.ScheduleParent(child)
		if !hasParent {
			logger.WithTaskAndPeerID(child.Task.TaskID, child.PeerID).Warnf("handlePeerLeave: there is no available parent，reschedule it in one second")
			s.waitScheduleParentPeerQueue.AddAfter(child, time.Second)
			continue
		}
		if child.PacketChan == nil {
			logger.Debugf("handlePeerLeave: there is no packet chan with peer %s", child.PeerID)
			continue
		}
		child.PacketChan <- constructSuccessPeerPacket(child, parent, candidates)
	}
	s.peerManager.Delete(peer.PeerID)
}

func handleReplaceParent(peer *types.Peer, s *state) {
	parent, candidates, hasParent := s.sched.ScheduleParent(peer)
	if !hasParent {
		logger.Errorf("handleReplaceParent: failed to schedule parent to peer %s", peer.PeerID)
		//peer.PacketChan <- constructFailPeerPacket(peer, dfcodes.SchedWithoutParentPeer)
		s.waitScheduleParentPeerQueue.AddAfter(peer, time.Second)
		return
	}
	if peer.PacketChan == nil {
		logger.Errorf("handleReplaceParent: there is no packet chan with peer %s", peer.PeerID)
		return
	}
	peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
}

func handleSeedTaskFail(task *types.Task) {
	if task.IsFail() {
		task.ListPeers().Range(func(data sortedlist.Item) bool {
			peer := data.(*types.Peer)
			if peer.PacketChan == nil {
				logger.Debugf("taskSeedFailEvent: there is no packet chan with peer %s", peer.PeerID)
				return true
			}
			peer.PacketChan <- constructFailPeerPacket(peer, dfcodes.CdnError)
			return true
		})
	}
}
