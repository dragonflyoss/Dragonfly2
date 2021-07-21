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

	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type event interface {
	hashKey() string
	apply(s *state)
}

type state struct {
	sched       scheduler.Scheduler
	peerManager daemon.PeerMgr
	cdnManager  daemon.CDNMgr
	work        worker
}

func newState(sched scheduler.Scheduler, peerManager daemon.PeerMgr, cdnManager daemon.CDNMgr, worker worker) *state {
	return &state{
		sched:       sched,
		peerManager: peerManager,
		cdnManager:  cdnManager,
		work:        worker,
	}
}

type peerScheduleParentEvent struct {
	peer *types.Peer
}

var _ event = peerScheduleParentEvent{}

func (e peerScheduleParentEvent) apply(s *state) {
	parent := e.peer.GetParent()
	var candidates []*types.Peer
	if parent == nil {
		parent, candidates = s.sched.ScheduleParent(e.peer)
	}
	if e.peer.PacketChan == nil {
		logger.Errorf("report piece result: there is no packet chan associated with peer %s", e.peer.PeerID)
		return
	}
	e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parent, candidates)
}

func (e peerScheduleParentEvent) hashKey() string {
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
	parentPeer, ok := s.peerManager.Get(e.pr.DstPid)
	if ok {
		// if wrong record in scheduler, amend it according to pr
		e.peer.ReplaceParent(parentPeer)
	}
	if e.peer.PacketChan == nil {
		logger.Errorf("newPieceDownloadSuccessJob: there is no packet chan with peer %s", e.peer.PeerID)
		return
	}
	e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parentPeer, []*types.Peer{oldParent})
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
		s.work.send(peerLeaveEvent{peer: e.peer})
		return
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		s.work.send(peerReplaceParentEvent{peer: e.peer})
		return
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail, dfcodes.CdnTaskDownloadFail:
		if err := s.cdnManager.StartSeedTask(context.Background(), e.peer.Task, true); err != nil {
			s.work.send(taskSeedFailEvent{e.peer.Task})
			return
		}
	default:
		s.work.send(peerReplaceParentEvent{peer: e.peer})
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
	parent, candidates := s.sched.ScheduleParent(e.peer)
	if e.peer.PacketChan == nil {
		logger.Errorf("newPieceDownloadSuccessJob: there is no packet chan with peer %s", e.peer.PeerID)
		return
	}
	e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parent, candidates)
	return
}

var _ event = peerReplaceParentEvent{}

type taskSeedFailEvent struct {
	task *types.Task
}

var _ event = taskSeedFailEvent{}

func (e taskSeedFailEvent) apply(s *state) {
	if e.task.IsFail() {
		e.task.ListPeers().Range(func(data sortedlist.Item) bool {
			peer := data.(*types.Peer)
			if peer.PacketChan == nil {
				logger.Warnf("taskSeedFailEvent: there is no packet chan with peer %s", peer.PeerID)
				return true
			}
			peer.PacketChan <- constructFailPeerPacket(peer, dfcodes.CdnError)
			return true
		})
	}
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
	for _, child := range children {
		if child.PacketChan == nil {
			logger.Warnf("reportPeerSuccessResult: there is no packet chan with peer %s", e.peer.PeerID)
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
		parent, candidates := s.sched.ScheduleParent(child)
		if child.PacketChan == nil {
			logger.Warnf("reportPeerDownloadResult: there is no packet chan associated with peer %s", e.peer.PeerID)
			continue
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
	e.peer.MarkLeave()
	e.peer.ReplaceParent(nil)
	for _, child := range e.peer.GetChildren() {
		parent, candidates := s.sched.ScheduleParent(child)
		if child.PacketChan == nil {
			logger.Warnf("leave: there is no packet chan with peer %s", child.PeerID)
			continue
		}
		child.PacketChan <- constructSuccessPeerPacket(child, parent, candidates)
	}
	s.peerManager.Delete(e.peer.PeerID)
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
