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

package service

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

const (
	defaultSchedulerParentInterval = 500 * time.Millisecond
)

type Callback interface {
	ScheduleParent(context.Context, *entity.Peer)
	InitReport(context.Context, *entity.Peer)
	PieceSuccess(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PieceFail(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PeerSuccess(context.Context, *entity.Peer, *rpcscheduler.PeerResult)
	PeerFail(context.Context, *entity.Peer, *rpcscheduler.PeerResult)
	TaskSuccess(context.Context, *entity.Peer, *entity.Task, *rpcscheduler.PeerResult)
	TaskFail(context.Context, *entity.Task)
}

type callback struct {
	// Manager entity instance
	manager *manager.Manager

	// Scheduler instance
	scheduler scheduler.Scheduler
}

func newCallback(manager *manager.Manager, scheduler scheduler.Scheduler) Callback {
	return &callback{
		manager:   manager,
		scheduler: scheduler,
	}
}

func (c *callback) PeerSuccess(ctx context.Context, peer *entity.Peer, result *rpcscheduler.PeerResult) {
	if err := peer.FSM.Event(entity.PeerEventSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	children, ok := c.scheduler.ScheduleChildren(ctx, peer, set.NewSafeSet())
	if !ok {
		peer.Log.Error("schedule children failed")
	}

	for _, child := range children {
		stream, ok := child.LoadStream()
		if !ok {
			peer.Log.Errorf("load child %s peer stream failed", child.ID)
			continue
		}

		if err := stream.Send(constructSuccessPeerPacket(child, peer, nil)); err != nil {
			peer.Log.Errorf("child %s peer stream send faied: %v", child.ID, err)
		}
	}
}

func (c *callback) PeerFail(ctx context.Context, peer *entity.Peer, result *rpcscheduler.PeerResult) {
	if err := peer.FSM.Event(entity.PeerEventFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer
	blocklist := set.NewSafeSet()
	blocklist.Add(peer.ID)

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*entity.Peer)
		if !ok {
			return true
		}

		if !child.FSM.Is(entity.PeerStateRunning) {
			peer.Log.Infof("load child %s state %s, can not schedule parent", child.ID, child.FSM.Current())
			return true
		}

		go c.ScheduleParent(ctx, child, blocklist)
		return true
	})
}

func (c *callback) TaskSuccess(ctx context.Context, peer *entity.Peer, task *entity.Task, result *rpcscheduler.PeerResult) {
	// Conditions for the task to switch to the TaskStateSucceeded are:
	// 1. CDN downloads the resource successfully
	// 2. Dfdaemon back-to-source to download successfully
	if peer.Host.IsCDN || task.BackToSourcePeers.Contains(peer) {
		if task.FSM.Is(entity.TaskEventSucceeded) {
			task.Log.Info("task fsm has been succeeded")
			return
		}

		if err := task.FSM.Event(entity.TaskEventSucceeded); err != nil {
			task.Log.Errorf("task fsm event failed: %v", err)
			return
		}

		// Update task's resource total piece count and content length
		task.TotalPieceCount.Store(result.TotalPieceCount)
		task.ContentLength.Store(result.ContentLength)
	}
}

func (c *callback) TaskFail(ctx context.Context, task *entity.Task) {
	if err := task.FSM.Event(entity.TaskEventFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}

	task.Peers.Range(func(_, value interface{}) bool {
		peer, ok := value.(*entity.Peer)
		if !ok {
			return true
		}

		if peer.FSM.Is(entity.PeerStateRunning) {
			// Reporting peer needs to back to source
			if task.CanBackToSource() {
				if !task.BackToSourcePeers.Contains(peer) {
					if ok := peer.StopStream(base.Code_SchedNeedBackSource); ok {
						if err := peer.FSM.Event(entity.PeerEventDownloadFromBackToSource); err != nil {
							task.Log.Errorf("peer fsm event failed: %v", err)
						}
						task.BackToSourcePeers.Add(peer)

						if task.FSM.Is(entity.TaskEventDownloadFromBackToSource) {
							task.Log.Info("task fsm has been succeeded")
							return true
						}

						if err := task.FSM.Event(entity.TaskEventDownloadFromBackToSource); err != nil {
							task.Log.Errorf("task fsm event failed: %v", err)
						}
					}

					return true
				}

				return true
			}

			// Reporting peer task failed
			peer.StopStream(base.Code_SchedTaskStatusError)
			if err := peer.FSM.Event(entity.PeerEventFailed); err != nil {
				task.Log.Errorf("peer fsm event failed: %v", err)
			}

			return true
		}

		return true
	})
}

func (c *callback) ScheduleParent(ctx context.Context, peer *entity.Peer, blocklist set.SafeSet) {
	for {
		stream, ok := peer.LoadStream()
		if !ok {
			peer.Log.Error("load peer stream failed")
			return
		}

		parent, candidateParents, ok := c.scheduler.ScheduleParent(ctx, peer, blocklist)
		if ok {
			if err := stream.Send(constructSuccessPeerPacket(peer, parent, candidateParents)); err != nil {
				peer.Log.Errorf("peer stream send faied: %v", err)
				return
			}
			return
		}

		// Sleep to avoid hot looping
		time.Sleep(defaultSchedulerParentInterval)
	}
}

func constructSuccessPeerPacket(peer *entity.Peer, parent *entity.Peer, candidateParents []*entity.Peer) *rpcscheduler.PeerPacket {
	var stealPeers []*rpcscheduler.PeerPacket_DestPeer
	for _, candidateParent := range candidateParents {
		stealPeers = append(stealPeers, &rpcscheduler.PeerPacket_DestPeer{
			Ip:      candidateParent.Host.IP,
			RpcPort: candidateParent.Host.Port,
			PeerId:  candidateParent.ID,
		})
	}

	return &rpcscheduler.PeerPacket{
		TaskId: peer.Task.ID,
		SrcPid: peer.ID,
		// TODO(gaius-qi) Configure ParallelCount parameter in manager service
		ParallelCount: 1,
		MainPeer: &rpcscheduler.PeerPacket_DestPeer{
			Ip:      parent.Host.IP,
			RpcPort: parent.Host.Port,
			PeerId:  parent.ID,
		},
		StealPeers: stealPeers,
		Code:       base.Code_Success,
	}
}
