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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type event interface {
	apply(s *state)
}

type state struct {
	sched       scheduler.Scheduler
	peerManager daemon.PeerMgr
}

func newState(sched scheduler.Scheduler, peerManager daemon.PeerMgr) *state {
	return &state{
		sched:       sched,
		peerManager: peerManager,
	}
}

type peerLeaveEvent struct {
	peer *types.Peer
}

func (e *peerLeaveEvent) apply(s state) {
	e.peer.SetStatus(types.PeerStatusLeaveNode)
	e.peer.ReplaceParent(nil)
	for _, child := range e.peer.GetChildren() {
		parent, candidates := s.sched.ScheduleParent(child)
		if e.peer.PacketChan == nil {
			logger.Warnf("leave: there is no packet chan with peer %s", e.peer.PeerID)
			continue
		}
		e.peer.PacketChan <- constructSuccessPeerPacket(e.peer, parent, candidates)
	}
	s.peerManager.Delete(e.peer.PeerID)
}

type peerDownloadSuccessEvent struct {
	peer *types.Peer
}

func (e *peerDownloadSuccessEvent) apply(s state) {

}

type peerDownloadFailEvent struct {
	peer *types.Peer
}

func (e *peerDownloadFailEvent) apply(s state) {

}

type peerDownloadPieceSuccessEvent struct {
}

func (e *peerDownloadPieceSuccessEvent) apply(s state) {

}

type peerDownloadPieceFailEvent struct {
}

func (e *peerDownloadPieceFailEvent) apply(s state) {

}
