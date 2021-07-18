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
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/structure/sortedlist"
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

// todo refactor job to event
// handle peer leave
func (factory *JobFactory) NewHandleLeaveJob(peer *types.Peer) func() {
	peer.Touch()
	return func() {
		peer.SetStatus(types.PeerStatusLeaveNode)
		peer.ReplaceParent(nil)
		for _, child := range peer.GetChildren() {
			parent, candidates := factory.scheduler.ScheduleParent(child)
			if peer.PacketChan == nil {
				logger.Warnf("leave: there is no packet chan with peer %s", peer.PeerID)
				continue
			}
			peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
		}
		factory.peerManager.Delete(peer.PeerID)
	}
}

// handle peer result
func (factory *JobFactory) NewHandleReportPeerResultJob(peer *types.Peer, result *schedulerRPC.PeerResult) func() {
	peer.Touch()
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
				parent, candidates := factory.scheduler.ScheduleParent(child)
				if child.PacketChan == nil {
					logger.Warnf("reportPeerResult: there is no packet chan associated with peer %s", peer.PeerID)
					continue
				}
				child.PacketChan <- constructSuccessPeerPacket(child, parent, candidates)
			}
			factory.peerManager.Delete(result.PeerId)
		}
	}
}

// handle report piece result
func (factory *JobFactory) NewHandleReportPieceResultJob(peer *types.Peer, pr *schedulerRPC.PieceResult) func() {
	peer.Touch()
	// todo task status check
	if pr.PieceNum == common.ZeroOfPiece {
		return func() {
			parent := peer.GetParent()
			var candidates []*types.Peer
			if parent == nil {
				parent, candidates = factory.scheduler.ScheduleParent(peer)
			}
			if peer.PacketChan == nil {
				logger.Errorf("report piece result: there is no packet chan associated with peer %s", peer.PeerID)
				return
			}
			peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
		}
	}
	if pr.Success {
		return factory.newPieceDownloadSuccessJob(peer, pr)
	}
	switch pr.Code {
	case dfcodes.Success:
		return factory.newPieceDownloadSuccessJob(peer, pr)
	case dfcodes.PeerTaskNotFound:
		destPeer, ok := factory.peerManager.Get(pr.DstPid)
		if ok {
			return factory.NewHandleLeaveJob(destPeer)
		}
	case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
		return factory.newPieceDownloadFailJob(peer, pr)
	case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail:
		if err := factory.cdnManager.StartSeedTask(context.Background(), peer.Task, true); err != nil {
			return factory.NewHandleFailSeedTaskJob(peer.Task)
		}
	default:
		return nil
	}
	return nil
}

func (factory *JobFactory) newPieceDownloadSuccessJob(peer *types.Peer, pr *schedulerRPC.PieceResult) func() {
	return func() {
		if pr.Success {
			peer.AddPieceInfo(pr.FinishedCount, int(pr.EndTime-pr.BeginTime))
			oldParent := peer.GetParent()
			parentPeer, ok := factory.peerManager.Get(pr.DstPid)
			if ok {
				// if wrong record in scheduler, amend it according to pr
				peer.ReplaceParent(parentPeer)
			}
			if peer.PacketChan == nil {
				logger.Errorf("newPieceDownloadSuccessJob: there is no packet chan with peer %s", peer.PeerID)
				return
			}
			peer.PacketChan <- constructSuccessPeerPacket(peer, parentPeer, []*types.Peer{oldParent})
			return
		}
	}
}

func (factory *JobFactory) NewHandleFailSeedTaskJob(task *types.Task) func() {
	return func() {
		if task.IsFail() {
			task.ListPeers().Range(func(data sortedlist.Item) bool {
				peer := data.(*types.Peer)
				if peer.PacketChan == nil {
					logger.Warnf("reportPeerResult: there is no packet chan with peer %s", peer.PeerID)
					return true
				}
				peer.PacketChan <- constructFailPeerPacket(peer, dfcodes.CdnError)
				return true
			})
		}
	}
}

func (factory *JobFactory) newPieceDownloadFailJob(peer *types.Peer, pr *schedulerRPC.PieceResult) func() {
	return func() {
		parent, candidates := factory.scheduler.ScheduleParent(peer)
		if peer.PacketChan == nil {
			logger.Errorf("newPieceDownloadSuccessJob: there is no packet chan with peer %s", peer.PeerID)
			return
		}
		peer.PacketChan <- constructSuccessPeerPacket(peer, parent, candidates)
		return
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

func constructFailPeerPacket(peer *types.Peer, errCode base.Code) *schedulerRPC.PeerPacket {
	return &schedulerRPC.PeerPacket{
		TaskId: peer.Task.TaskID,
		SrcPid: peer.PeerID,
		Code:   errCode,
	}
}
