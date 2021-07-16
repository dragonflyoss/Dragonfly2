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
	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type missionFactory struct {

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

func newMissionFactory(scheduler scheduler.Scheduler, cdnManager daemon.CDNMgr, taskManager daemon.TaskMgr, hostManager daemon.HostMgr,
	peerManager daemon.PeerMgr) (*missionFactory, error) {

	return &missionFactory{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		scheduler:   scheduler,
	}, nil
}

func (factory *missionFactory) NewHandleLeaveMission(target *schedulerRPC.PeerTarget) func() {
	return func() {
		peer, _ := factory.peerManager.Get(target.PeerId)
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

func (factory *missionFactory) NewHandleReportPeerResultMission(result *schedulerRPC.PeerResult) func() {
	return func() {
		peer, _ := factory.peerManager.Get(result.PeerId)
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

func (factory *missionFactory) NewHandleReportPieceResultMission(pr *schedulerRPC.PieceResult) func() {
	if factory.processErrorCode(pr) {
		return func() {

		}
	}
	return func() {
		//peer, ok := factory.peerManager.Get(pr.SrcPid)
		//if pr == nil || factory.processErrorCode(pr) {
		//	return
		//}

	}
}

func (factory *missionFactory) processErrorCode(pr *schedulerRPC.PieceResult) (stop bool) {
	//code := pr.Code
	//switch code {
	//case dfcodes.Success:
	//	return
	//case dfcodes.PeerTaskNotFound:
	//	peer, ok := factory.peerManager.Get(pr.SrcPid)
	//	if ok {
	//		worker.Submit(NewHandleLeaveTask(worker, &schedulerRPC.PeerTarget{
	//			TaskId: peer.Task.TaskID,
	//			PeerId: peer.PeerID,
	//		}))
	//	}
	//	return true
	//case dfcodes.ClientPieceRequestFail, dfcodes.ClientPieceDownloadFail:
	//	peerTask, _ := worker.peerManager.Get(pr.SrcPid)
	//	if peerTask != nil {
	//		factory.Submit(new())
	//		peerTask.SetStatus(types.PeerStatusNeedParent)
	//		worker.sendJob(peerTask)
	//	}
	//	return true
	//case dfcodes.CdnTaskNotFound, dfcodes.CdnError, dfcodes.CdnTaskRegistryFail:
	//	peerTask, _ := worker.peerManager.Get(pr.SrcPid)
	//	if peerTask != nil {
	//		peerTask.SetStatus(types.PeerStatusNeedParent)
	//		w.sendJob(peerTask)
	//		task := peerTask.Task
	//		if task != nil {
	//			if task.CDNError != nil {
	//				go safe.Call(func() { peerTask.SendError(task.CDNError) })
	//			} else {
	//				worker.CDNManager.TriggerTask(task, w.schedulerService.TaskManager.PeerTask.CDNCallback)
	//			}
	//		}
	//	}
	//	return true
	//}
	return true
}

func constructSuccessPeerPacket(peer *types.PeerNode, parent *types.PeerNode, candidates []*types.PeerNode) *schedulerRPC.PeerPacket {
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
