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

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Callback interface {
	BeginOfPiece(context.Context, *resource.Peer)
	EndOfPiece(context.Context, *resource.Peer)
	PieceSuccess(context.Context, *resource.Peer, *rpcscheduler.PieceResult)
	PieceFail(context.Context, *resource.Peer, *rpcscheduler.PieceResult)
	PeerSuccess(context.Context, *resource.Peer)
	PeerFail(context.Context, *resource.Peer)
	PeerLeave(context.Context, *resource.Peer)
	TaskSuccess(context.Context, *resource.Task, *rpcscheduler.PeerResult)
	TaskFail(context.Context, *resource.Task)
}

type callback struct {
	// Resource interface
	resource resource.Resource

	// Scheduler interface
	scheduler scheduler.Scheduler

	// Scheduelr service config
	config *config.Config
}

func newCallback(cfg *config.Config, resource resource.Resource, scheduler scheduler.Scheduler) Callback {
	return &callback{
		config:    cfg,
		resource:  resource,
		scheduler: scheduler,
	}
}

func (c *callback) BeginOfPiece(ctx context.Context, peer *resource.Peer) {
	switch peer.FSM.Current() {
	case resource.PeerStateBackToSource:
		// Back to the source download process, peer directly returns
		peer.Log.Info("peer back to source")
		return
	case resource.PeerStateReceivedTiny:
		// When the task is tiny,
		// the peer has already returned to piece data when registering
		peer.Log.Info("file type is tiny, peer has already returned to piece data when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}
	case resource.PeerStateReceivedSmall:
		// When the task is small,
		// the peer has already returned to the parent when registering
		peer.Log.Info("file type is small, peer has already returned to the parent when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}
	case resource.PeerStateReceivedNormal:
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}

		// It’s not a case of back-to-source or small task downloading,
		// to help peer to schedule the parent node
		blocklist := set.NewSafeSet()
		blocklist.Add(peer.ID)
		c.scheduler.ScheduleParent(ctx, peer, blocklist)
	default:
		peer.Log.Warnf("peer state is %s when receive the begin of piece", peer.FSM.Current())
	}
}

func (c *callback) EndOfPiece(ctx context.Context, peer *resource.Peer) {}

func (c *callback) PieceSuccess(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Update peer piece info
	peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.AppendPieceCost(int64(piece.EndTime - piece.BeginTime))

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
	}
}

func (c *callback) PieceFail(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Failed to download piece back-to-source
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Log.Error("peer back to source finished with fail piece")
		return
	}

	// If parent can not found, reschedule parent.
	parent, ok := c.resource.PeerManager().Load(piece.DstPid)
	if !ok {
		peer.Log.Errorf("can not found parent %s and reschedule", piece.DstPid)
		c.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet())
		return
	}

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node
	switch piece.Code {
	case base.Code_ClientPieceDownloadFail, base.Code_PeerTaskNotFound, base.Code_CDNError, base.Code_CDNTaskDownloadFail:
		if err := parent.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			break
		}
	case base.Code_ClientPieceNotFound:
		// Dfdaemon downloading piece data from parent returns http error code 404.
		// If the parent is not a CDN, reschedule parent for peer.
		// If the parent is a CDN, scheduler need to trigger CDN to download again.
		if !parent.Host.IsCDN {
			peer.Log.Infof("parent %s is not cdn", piece.DstPid)
			break
		}

		peer.Log.Infof("parent %s is cdn", piece.DstPid)
		fallthrough
	case base.Code_CDNTaskNotFound:
		c.PeerFail(ctx, parent)
		go func() {
			parent.Log.Info("cdn restart seed task")
			cdnPeer, endOfPiece, err := c.resource.CDN().TriggerTask(context.Background(), parent.Task)
			if err != nil {
				peer.Log.Errorf("retrigger task failed: %v", err)
				c.TaskFail(ctx, parent.Task)
				return
			}

			c.TaskSuccess(ctx, cdnPeer.Task, endOfPiece)
			c.PeerSuccess(ctx, cdnPeer)
		}()
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
		return
	}

	blocklist := set.NewSafeSet()
	blocklist.Add(parent.ID)

	c.scheduler.ScheduleParent(ctx, peer, blocklist)
}

func (c *callback) PeerSuccess(ctx context.Context, peer *resource.Peer) {
	// If the peer type is tiny and back-to-source,
	// it need to directly download the tiny file and store the data in task DirectPiece
	if peer.Task.SizeScope() == base.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
		data, err := peer.DownloadTinyFile()
		if err == nil && len(data) == int(peer.Task.ContentLength.Load()) {
			// Tiny file downloaded successfully
			peer.Task.DirectPiece = data
		} else {
			peer.Log.Warnf("download tiny file length is %d, task content length is %d, download is failed: %v", len(data), peer.Task.ContentLength.Load(), err)
		}
	}

	if err := peer.FSM.Event(resource.PeerEventDownloadSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}
}

func (c *callback) PeerFail(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer
	blocklist := set.NewSafeSet()
	blocklist.Add(peer.ID)

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		c.scheduler.ScheduleParent(ctx, child, blocklist)
		return true
	})
}

func (c *callback) PeerLeave(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		// Reschedule a new parent to children of peer to exclude the current leave peer
		blocklist := set.NewSafeSet()
		blocklist.Add(peer.ID)

		c.scheduler.ScheduleParent(ctx, child, blocklist)
		return true
	})

	peer.DeleteParent()
	c.resource.PeerManager().Delete(peer.ID)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource successfully
// 2. Dfdaemon back-to-source to download successfully
func (c *callback) TaskSuccess(ctx context.Context, task *resource.Task, result *rpcscheduler.PeerResult) {
	if task.FSM.Is(resource.TaskStateSucceeded) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}

	// Update task's resource total piece count and content length
	task.TotalPieceCount.Store(result.TotalPieceCount)
	task.ContentLength.Store(result.ContentLength)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource falied
// 2. Dfdaemon back-to-source to download failed
func (c *callback) TaskFail(ctx context.Context, task *resource.Task) {
	if task.FSM.Is(resource.TaskStateFailed) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}
}
