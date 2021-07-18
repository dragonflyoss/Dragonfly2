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
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type JobFactory struct {

	// cdn mgr
	cdnManager daemon.CDNMgr
	// task mgr
	taskManager daemon.TaskMgr
	// host mgr
	hostManager daemon.HostMgr
	// Peer mgr
	peerManager daemon.PeerMgr

	scheduler scheduler.Scheduler
}

func newJobFactory(scheduler scheduler.Scheduler, cdnManager daemon.CDNMgr, taskManager daemon.TaskMgr, hostManager daemon.HostMgr,
	peerManager daemon.PeerMgr) (*JobFactory, error) {

	return &JobFactory{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		scheduler:   scheduler,
	}, nil
}

func (factory *JobFactory) NewHandleLeaveJob(peer *types.Peer, target *schedulerRPC.PeerTarget) func() {
	return func() {
		peer.SetStatus(types.PeerStatusLeaveNode)
		peer.ReplaceParent(nil)
		for _, child := range peer.GetChildren() {
			parent, candidates := factory.scheduler.ScheduleParent(child, 10)
			if peer.PacketChan == nil {
				logger.Warnf("leave: there is no packet chan with peer %s", peer.PeerID)
				continue
			}
			peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
		}
		factory.peerManager.Delete(target.PeerId)
	}
}

func (factory *JobFactory) NewHandleReportPeerResultJob(peer *types.Peer, result *schedulerRPC.PeerResult) func() {
	return func() {
		peer.ReplaceParent(nil)
		if result.Success {
			peer.SetStatus(types.PeerStatusSuccess)
			children := factory.scheduler.ScheduleChildren(peer)
			for _, child := range children {
				if child.PacketChan == nil {
					logger.Warnf("reportPeerResult: there is no packet chan with peer %s", peer.PeerID)
					continue
				}
				child.PacketChan <- constructSuccessPeerPacket(child, peer, nil)
			}
		} else {
			peer.SetStatus(types.PeerStatusBadNode)
			for _, child := range peer.GetChildren() {
				parent, candidates := factory.scheduler.ScheduleParent(child, 10)
				if child.PacketChan == nil {
					logger.Warnf("reportPeerResult: there is no packet chan with peer %s", peer.PeerID)
					continue
				}
				child.PacketChan <- constructSuccessPeerPacket(child, parent, candidates)
			}
			factory.peerManager.Delete(result.PeerId)
		}
	}
}

func (factory *JobFactory) NewHandleReportPieceResultJob(peer *types.Peer, pr *schedulerRPC.PieceResult) func() {
	switch pr.Code {
	case dfcodes.Success:
		return func() {
			if pr.Success {
				peer.AddPieceStatus(pr)
				return
			}
		}
	case dfcodes.PeerTaskNotFound:
		return func() {
			factory.cdnManager.StartSeedTask(context.Background(), types.NewTask())
		}
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		return func() {

		}
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail:
		return func() {

		}
	}

	return func() {

	}
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
