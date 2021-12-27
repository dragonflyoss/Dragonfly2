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
	defaultSchedulerParentInterval = 100 * time.Millisecond
	defalutReschedulerLimit        = 10
)

type Callback interface {
	ScheduleSendParent(context.Context, *entity.Peer, set.SafeSet)
	ScheduleSendChildren(context.Context, *entity.Peer, set.SafeSet)
	BeginOfPiece(context.Context, *entity.Peer)
	EndOfPiece(context.Context, *entity.Peer)
	PieceSuccess(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PieceFail(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PeerSuccess(context.Context, *entity.Peer)
	PeerFail(context.Context, *entity.Peer)
	PeerLeave(context.Context, *entity.Peer)
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

// Schedule parent for peer, peer status must be PeerStateRunning
func (c *callback) ScheduleSendParent(ctx context.Context, peer *entity.Peer, blocklist set.SafeSet) {
	var n int

	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return
		default:
		}

		stream, ok := peer.LoadStream()
		if !ok {
			peer.Log.Error("load peer stream failed")
			return
		}

		if n > defalutReschedulerLimit {
			if peer.Task.CanBackToSource() {
				if ok := peer.StopStream(base.Code_SchedNeedBackSource); !ok {
					return
				}

				if err := peer.FSM.Event(entity.PeerEventDownloadFromBackToSource); err != nil {
					peer.Log.Errorf("peer fsm event failed: %v", err)
					return
				}

				if peer.Task.FSM.Is(entity.PeerStatePending) {
					if err := peer.Task.FSM.Event(entity.TaskEventDownload); err != nil {
						peer.Task.Log.Errorf("task fsm event failed: %v", err)
						return
					}
				}

				// If the peer downloads back-to-source, its parent needs to be deleted
				peer.DeleteParent()
				peer.Task.Log.Info("peer back to source successfully")
				return
			}

			// Handle peer failed
			if ok := peer.StopStream(base.Code_SchedTaskStatusError); !ok {
				peer.Log.Error("stop stream failed")
			}

			if err := peer.FSM.Event(entity.PeerEventFinished); err != nil {
				peer.Log.Errorf("peer fsm event failed: %v", err)
				return
			}
			return
		}

		parent, candidateParents, ok := c.scheduler.ScheduleParent(ctx, peer, blocklist)
		if !ok {
			n++
			peer.Log.Infof("reschedule parent %d times failed", n)

			// Sleep to avoid hot looping
			time.Sleep(defaultSchedulerParentInterval)
			continue
		}

		// After scheduling, the parent is the same, so there is no need to reschedule
		if oldParent, ok := peer.LoadParent(); ok && oldParent == parent {
			peer.Log.Infof("reschedule same parent %s", parent.ID)
			return
		}

		if parent.Host.FreeUploadLoad() > 0 {
			if err := stream.Send(constructSuccessPeerPacket(peer, parent, candidateParents)); err != nil {
				peer.Log.Error(err)
				return
			}

			peer.ReplaceParent(parent)
			peer.Log.Infof("reschedule parent %d times succeeded, replace parent to %s", n, parent.ID)
			return
		}
	}
}

func (c *callback) ScheduleSendChildren(ctx context.Context, peer *entity.Peer, blocklist set.SafeSet) {
	children, ok := c.scheduler.ScheduleChildren(ctx, peer, blocklist)
	if !ok {
		peer.Log.Error("schedule children failed")
		return
	}

	for _, child := range children {
		stream, ok := child.LoadStream()
		if !ok {
			peer.Log.Errorf("load child %s peer stream failed", child.ID)
			continue
		}

		// After scheduling, the parent is the same, so there is no need to reschedule
		if oldParent, ok := child.LoadParent(); ok && oldParent == peer {
			peer.Log.Infof("reschedule same parent %s", peer.ID)
			continue
		}

		if peer.Host.FreeUploadLoad() > 0 {
			if err := stream.Send(constructSuccessPeerPacket(child, peer, nil)); err != nil {
				peer.Log.Errorf("child %s peer stream send faied: %v", child.ID, err)
				continue
			}

			child.ReplaceParent(peer)
			child.Log.Infof("replace parent to %s", peer.ID)
		}
	}
}

func (c *callback) BeginOfPiece(ctx context.Context, peer *entity.Peer) {
	// Back to the source download process, peer directly returns
	if peer.FSM.Is(entity.PeerStateBackToSource) {
		peer.Log.Info("peer back to source")
		return
	}

	// It’s not a case of back-to-source downloading,
	// to help peer to schedule the parent node
	if err := peer.FSM.Event(entity.PeerEventDownload); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	c.ScheduleSendParent(ctx, peer, set.NewSafeSet())
}

func (c *callback) EndOfPiece(ctx context.Context, peer *entity.Peer) {
	if err := peer.FSM.Event(entity.PeerEventFinished); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}
}

func (c *callback) PieceFail(ctx context.Context, peer *entity.Peer, piece *rpcscheduler.PieceResult) {
	// Failed to download piece back-to-source, switch peer status to PeerEventFinished
	if peer.FSM.Is(entity.PeerStateBackToSource) {
		if err := peer.FSM.Event(entity.PeerEventFinished); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}

		peer.Log.Error("peer back to source finished with fail piece")
		return
	}

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node
	switch piece.Code {
	case base.Code_ClientWaitPieceReady:
		peer.Log.Error("peer report error code Code_ClientWaitPieceReady")
		return
	case base.Code_PeerTaskNotFound, base.Code_CDNTaskNotFound, base.Code_CDNError, base.Code_CDNTaskDownloadFail:
		peer.Log.Errorf("peer report cdn error code: %v", piece.Code)
		if parent, ok := c.manager.Peer.Load(piece.DstPid); ok {
			if err := parent.FSM.Event(entity.PeerEventFailed); err != nil {
				peer.Log.Errorf("peer fsm event failed: %v", err)
				break
			}
		}
	default:
		peer.Log.Warnf("unknow report code: %v", piece.Code)
	}

	// Peer state is PeerStateRunning will be rescheduled
	if !peer.FSM.Is(entity.PeerStateRunning) {
		peer.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
		return
	}

	blocklist := set.NewSafeSet()
	if parent, ok := c.manager.Peer.Load(piece.DstPid); ok {
		blocklist.Add(parent.ID)
	}

	c.ScheduleSendParent(ctx, peer, blocklist)
}

func (c *callback) PieceSuccess(ctx context.Context, peer *entity.Peer, piece *rpcscheduler.PieceResult) {
	// Update peer piece info
	peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.PieceCosts.Add(piece.EndTime - piece.BeginTime)

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info
	if peer.FSM.Is(entity.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
	}
}

func (c *callback) PeerSuccess(ctx context.Context, peer *entity.Peer) {
	if err := peer.FSM.Event(entity.PeerEventSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	// Schedule children to peer
	go c.ScheduleSendChildren(ctx, peer, set.NewSafeSet())
}

func (c *callback) PeerFail(ctx context.Context, peer *entity.Peer) {
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

		// Children state is PeerStateRunning will be rescheduled
		if !child.FSM.Is(entity.PeerStateRunning) {
			child.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
			return true
		}

		go c.ScheduleSendParent(ctx, child, blocklist)
		return true
	})
}

func (c *callback) PeerLeave(ctx context.Context, peer *entity.Peer) {
	if err := peer.FSM.Event(entity.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*entity.Peer)
		if !ok {
			return true
		}

		// Children state is PeerStateRunning will be rescheduled
		if !child.FSM.Is(entity.PeerStateRunning) {
			child.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
			return true
		}

		// Reschedule a new parent to children of peer to exclude the current leave peer
		blocklist := set.NewSafeSet()
		blocklist.Add(peer.ID)

		c.ScheduleSendParent(ctx, child, blocklist)
		return true
	})

	peer.DeleteParent()
	c.manager.Peer.Delete(peer.ID)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource successfully
// 2. Dfdaemon back-to-source to download successfully
func (c *callback) TaskSuccess(ctx context.Context, peer *entity.Peer, task *entity.Task, endOfPiece *rpcscheduler.PeerResult) {
	if err := task.FSM.Event(entity.TaskEventSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}

	// Update task's resource total piece count and content length
	task.TotalPieceCount.Store(endOfPiece.TotalPieceCount)
	task.ContentLength.Store(endOfPiece.ContentLength)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource falied
// 2. Dfdaemon back-to-source to download failed
func (c *callback) TaskFail(ctx context.Context, task *entity.Task) {
	if err := task.FSM.Event(entity.TaskEventFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
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
