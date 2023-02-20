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

//go:generate mockgen -destination mocks/scheduling_mock.go -source scheduling.go -package mocks

package scheduling

import (
	"context"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling/evaluator"
)

type Scheduling interface {
	// ScheduleCandidateParentsForNormalPeer schedules candidate parents to the normal peer.
	// Used only in v2 version of the grpc.
	ScheduleCandidateParentsForNormalPeer(context.Context, *resource.Peer, set.SafeSet[string])

	// ScheduleCandidateParentForSmallPeer schedules candidate parent to the small peer.
	// Used only in v2 version of the grpc.
	ScheduleCandidateParentForSmallPeer(context.Context, *resource.Peer, set.SafeSet[string]) (*resource.Peer, bool)

	// ScheduleParentAndCandiateParentsForNormalPeer schedules a parent and candidate parents to the normal peer.
	// Used only in v1 version of the grpc.
	ScheduleParentsForNormalPeer(context.Context, *resource.Peer, set.SafeSet[string])

	// FindCandidateParents finds candidate parents for the peer.
	FindCandidateParents(context.Context, *resource.Peer, set.SafeSet[string]) ([]*resource.Peer, bool)
}

type scheduling struct {
	// Evaluator interface.
	evaluator evaluator.Evaluator

	// Scheduler configuration.
	config *config.SchedulerConfig

	// Scheduler dynamic configuration.
	dynconfig config.DynconfigInterface
}

func New(cfg *config.SchedulerConfig, dynconfig config.DynconfigInterface, pluginDir string) Scheduling {
	return &scheduling{
		evaluator: evaluator.New(cfg.Algorithm, pluginDir),
		config:    cfg,
		dynconfig: dynconfig,
	}
}

// ScheduleCandidateParentsForNormalPeer schedules candidate parents to the normal peer.
// Used only in v2 version of the grpc.
func (s *scheduling) ScheduleCandidateParentsForNormalPeer(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet[string]) {
	var n int
	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return
		default:
		}

		if peer.Task.CanBackToSource() {
			// If peer needs back-to-source, send back-to-source response to peer.
			if peer.NeedBackToSource.Load() {
				stream, loaded := peer.LoadAnnouncePeerStream()
				if !loaded {
					peer.Log.Error("load stream failed")
					return
				}

				// Send back-to-source response to peer with reason.
				reason := "peer needs to back-to-source"
				if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
					Response: &schedulerv2.AnnouncePeerResponse_NeedBackToSourceResponse{
						NeedBackToSourceResponse: &schedulerv2.NeedBackToSourceResponse{
							Reason: reason,
						},
					},
				}); err != nil {
					peer.Log.Error(err)
					return
				}
				peer.Log.Infof(reason)

				if err := peer.FSM.Event(ctx, resource.PeerEventDownloadBackToSource); err != nil {
					peer.Log.Errorf("peer fsm event failed: %s", err.Error())
					return
				}

				// If the task state is TaskStateFailed,
				// peer back-to-source and reset task state to TaskStateRunning.
				if peer.Task.FSM.Is(resource.TaskStateFailed) {
					if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
						peer.Task.Log.Errorf("task fsm event failed: %s", err.Error())
						return
					}
				}

				return

			}

			// If the scheduling exceeds the RetryBackToSourceLimit, send back-to-source response to peer.
			if n >= s.config.RetryBackToSourceLimit {
				stream, loaded := peer.LoadAnnouncePeerStream()
				if !loaded {
					peer.Log.Error("load stream failed")
					return
				}

				// Send back-to-source response to peer with reason.
				reason := fmt.Sprintf("schedule peer in %d times", n)
				if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
					Response: &schedulerv2.AnnouncePeerResponse_NeedBackToSourceResponse{
						NeedBackToSourceResponse: &schedulerv2.NeedBackToSourceResponse{
							Reason: reason,
						},
					},
				}); err != nil {
					peer.Log.Error(err)
					return
				}
				peer.Log.Infof(reason)

				if err := peer.FSM.Event(ctx, resource.PeerEventDownloadBackToSource); err != nil {
					peer.Log.Errorf("peer fsm event failed: %s", err.Error())
					return
				}

				// If the task state is TaskStateFailed,
				// peer back-to-source and reset task state to TaskStateRunning.
				if peer.Task.FSM.Is(resource.TaskStateFailed) {
					if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
						peer.Task.Log.Errorf("task fsm event failed: %s", err.Error())
						return
					}
				}

				return
			}
		}

		// Handle peer schedule failed.
		if n >= s.config.RetryLimit {
			stream, loaded := peer.LoadAnnouncePeerStream()
			if !loaded {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer schedule failed.
			reason := fmt.Sprintf("schedule parent exceeded the limit %d times", s.config.RetryLimit)
			if err := stream.Send(&schedulerv2.AnnouncePeerResponse{
				Response: &schedulerv2.AnnouncePeerResponse_NeedBackToSourceResponse{
					NeedBackToSourceResponse: &schedulerv2.NeedBackToSourceResponse{
						Reason: reason,
					},
				},
			}); err != nil {
				peer.Log.Error(err)
				return
			}

			peer.Log.Error(reason)
			return
		}

		// Delete inedges of vertex.
		if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
			n++
			peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		candidateParents, found := s.FindCandidateParents(ctx, peer, blocklist)
		if !found {
			n++
			peer.Log.Infof("schedule parent failed in %d times ", n)

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		// Load stream of the report piece result.
		stream, loaded := peer.LoadAnnouncePeerStream()
		if !loaded {
			n++
			peer.Log.Error("load peer stream failed")

			if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
				peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())
			}

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		if err := stream.Send(constructSuccessPeerPacket(s.dynconfig, peer, candidateParents[0], candidateParents[1:])); err != nil {
			n++
			peer.Log.Error(err)

			if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
				peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())
			}

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		peer.Log.Infof("schedule parent successfully in %d times", n+1)
		return
	}
}

// ScheduleCandidateParentForSmallPeer schedules candidate parent to the small peer.
// Used only in v2 version of the grpc.
func (s *scheduling) ScheduleCandidateParentForSmallPeer(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet[string]) (*resource.Peer, bool) {
	return nil, false
}

// ScheduleParentsForNormalPeer schedules a parent and candidate parents to a peer.
// Used only in v1 version of the grpc.
func (s *scheduling) ScheduleParentsForNormalPeer(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet[string]) {
	var n int
	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return
		default:
		}

		// If the scheduling exceeds the RetryBackToSourceLimit or peer needs back-to-source,
		// peer will download the task back-to-source.
		needBackToSource := peer.NeedBackToSource.Load()
		peer.Log.Infof("peer needs to back-to-source: %t", needBackToSource)
		if (n >= s.config.RetryBackToSourceLimit || needBackToSource) &&
			peer.Task.CanBackToSource() {
			stream, loaded := peer.LoadReportPieceResultStream()
			if !loaded {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer back-to-source.
			if err := stream.Send(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedNeedBackSource}); err != nil {
				peer.Log.Error(err)
				return
			}
			peer.Log.Infof("schedule peer back-to-source in %d times", n)

			if err := peer.FSM.Event(ctx, resource.PeerEventDownloadBackToSource); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return
			}

			// If the task state is TaskStateFailed,
			// peer back-to-source and reset task state to TaskStateRunning.
			if peer.Task.FSM.Is(resource.TaskStateFailed) {
				if err := peer.Task.FSM.Event(ctx, resource.TaskEventDownload); err != nil {
					peer.Task.Log.Errorf("task fsm event failed: %s", err.Error())
					return
				}
			}

			return
		}

		// Handle peer schedule failed.
		if n >= s.config.RetryLimit {
			stream, loaded := peer.LoadReportPieceResultStream()
			if !loaded {
				peer.Log.Error("load stream failed")
				return
			}

			// Notify peer schedule failed.
			if err := stream.Send(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedTaskStatusError}); err != nil {
				peer.Log.Error(err)
				return
			}
			peer.Log.Errorf("schedule parent exceeded the limit %d times", s.config.RetryLimit)
			return
		}

		// Delete inedges of vertex.
		if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
			n++
			peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		candidateParents, found := s.FindCandidateParents(ctx, peer, blocklist)
		if !found {
			n++
			peer.Log.Infof("schedule parent failed in %d times ", n)

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		// Load stream of the report piece result.
		stream, loaded := peer.LoadReportPieceResultStream()
		if !loaded {
			n++
			peer.Log.Error("load peer stream failed")

			if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
				peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())
			}

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		if err := stream.Send(constructSuccessPeerPacket(s.dynconfig, peer, candidateParents[0], candidateParents[1:])); err != nil {
			n++
			peer.Log.Error(err)

			if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
				peer.Log.Errorf("peer deletes inedges failed: %s", err.Error())
			}

			// Sleep to avoid hot looping.
			time.Sleep(s.config.RetryInterval)
			continue
		}

		peer.Log.Infof("schedule parent successfully in %d times", n+1)
		return
	}
}

// FindCandidateParents finds candidate parents for the peer.
func (s *scheduling) FindCandidateParents(ctx context.Context, peer *resource.Peer, blocklist set.SafeSet[string]) ([]*resource.Peer, bool) {
	// Only PeerStateRunning peers need to be rescheduled,
	// and other states including the PeerStateBackToSource indicate that
	// they have been scheduled.
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer state is %s, can not schedule parent", peer.FSM.Current())
		return []*resource.Peer{}, false
	}

	// Find the candidate parent that can be scheduled.
	candidateParents := s.filterCandidateParents(peer, blocklist)
	if len(candidateParents) == 0 {
		peer.Log.Info("can not find candidate parents")
		return []*resource.Peer{}, false
	}

	// Sort candidate parents by evaluation score.
	taskTotalPieceCount := peer.Task.TotalPieceCount.Load()
	sort.Slice(
		candidateParents,
		func(i, j int) bool {
			return s.evaluator.Evaluate(candidateParents[i], peer, taskTotalPieceCount) > s.evaluator.Evaluate(candidateParents[j], peer, taskTotalPieceCount)
		},
	)

	// Add edges between candidate parent and peer.
	var parentIDs []string
	for _, candidateParent := range candidateParents {
		parentIDs = append(parentIDs, candidateParent.ID)
	}

	if len(candidateParents) <= 0 {
		peer.Log.Info("can not add edges for vertex")
		return []*resource.Peer{}, false
	}

	peer.Log.Infof("schedule candidate parents is %#v", parentIDs)
	return candidateParents, true
}

// filterCandidateParents filters the candidate parents that can be scheduled.
func (s *scheduling) filterCandidateParents(peer *resource.Peer, blocklist set.SafeSet[string]) []*resource.Peer {
	filterParentLimit := config.DefaultSchedulerFilterParentLimit
	filterParentRangeLimit := config.DefaultSchedulerFilterParentRangeLimit
	if config, err := s.dynconfig.GetSchedulerClusterConfig(); err == nil {
		if config.FilterParentLimit > 0 {
			filterParentLimit = int(config.FilterParentLimit)
		}

		if config.FilterParentRangeLimit > 0 {
			filterParentRangeLimit = int(config.FilterParentRangeLimit)
		}
	}

	var (
		candidateParents   []*resource.Peer
		candidateParentIDs []string
	)
	for _, candidateParent := range peer.Task.LoadRandomPeers(uint(filterParentRangeLimit)) {
		// Parent length limit after filtering.
		if len(candidateParents) >= filterParentLimit {
			break
		}

		// Candidate parent is in blocklist.
		if blocklist.Contains(candidateParent.ID) {
			peer.Log.Debugf("parent %s is not selected because it is in blocklist", candidateParent.ID)
			continue
		}

		// Candidate parent can add edge with peer.
		if !peer.Task.CanAddPeerEdge(candidateParent.ID, peer.ID) {
			peer.Log.Debugf("can not add edge with parent %s", candidateParent.ID)
			continue
		}

		// Candidate parent host is not allowed to be the same as the peer host,
		// because dfdaemon cannot handle the situation
		// where two tasks are downloading and downloading each other.
		if peer.Host.ID == candidateParent.Host.ID {
			peer.Log.Debugf("parent %s host %s is the same as peer host", candidateParent.ID, candidateParent.Host.ID)
			continue
		}

		// Candidate parent is bad node.
		if s.evaluator.IsBadNode(candidateParent) {
			peer.Log.Debugf("parent %s is not selected because it is bad node", candidateParent.ID)
			continue
		}

		// Candidate parent can not find in dag.
		inDegree, err := peer.Task.PeerInDegree(candidateParent.ID)
		if err != nil {
			peer.Log.Debugf("can not find parent %s vertex in dag", candidateParent.ID)
			continue
		}

		// Conditions for candidate parent to be a parent:
		// 1. parent has parent.
		// 2. parent has been back-to-source.
		// 3. parent has been succeeded.
		// 4. parent is seed peer.
		isBackToSource := candidateParent.IsBackToSource.Load()
		if candidateParent.Host.Type == types.HostTypeNormal && inDegree == 0 && !isBackToSource &&
			!candidateParent.FSM.Is(resource.PeerStateSucceeded) {
			peer.Log.Debugf("parent %s is not selected, because its download state is %d %d %t %s",
				candidateParent.ID, inDegree, int(candidateParent.Host.Type), isBackToSource, candidateParent.FSM.Current())
			continue
		}

		// Candidate parent's free upload is empty.
		if candidateParent.Host.FreeUploadCount() <= 0 {
			peer.Log.Debugf("parent %s is not selected because its free upload is empty, upload limit is %d, upload count is %d",
				candidateParent.ID, candidateParent.Host.ConcurrentUploadLimit.Load(), candidateParent.Host.ConcurrentUploadCount.Load())
			continue
		}

		candidateParents = append(candidateParents, candidateParent)
		candidateParentIDs = append(candidateParentIDs, candidateParent.ID)
	}

	peer.Log.Infof("filter candidate parents is %#v", candidateParentIDs)
	return candidateParents
}

// constructSuccessNormalTaskResponse constructs scheduling successful response of the normal task.
// Used only in v2 version of the grpc.
func constructSuccessNormalTaskResponse(dynconfig config.DynconfigInterface, candidateParents []*resource.Peer) *schedulerv2.AnnouncePeerResponse_NormalTaskResponse {
	concurrentPieceCount := config.DefaultPeerConcurrentPieceCount
	if config, err := dynconfig.GetSchedulerClusterClientConfig(); err == nil && config.ConcurrentPieceCount > 0 {
		concurrentPieceCount = int(config.ConcurrentPieceCount)
	}

	var parents []*commonv2.Peer
	for _, candidateParent := range candidateParents {
		parent := &commonv2.Peer{
			Id:               candidateParent.ID,
			Priority:         candidateParent.Priority,
			Cost:             durationpb.New(candidateParent.Cost.Load()),
			State:            candidateParent.FSM.Current(),
			Host:             &commonv2.Host{},
			NeedBackToSource: candidateParent.NeedBackToSource.Load(),
			CreatedAt:        timestamppb.New(candidateParent.CreatedAt.Load()),
			UpdatedAt:        timestamppb.New(candidateParent.UpdatedAt.Load()),
		}

		// Set range to parent.
		if candidateParent.Range != nil {
			parent.Range = &commonv2.Range{
				Start:  candidateParent.Range.Start,
				Length: candidateParent.Range.Length,
			}
		}

		// Set pieces to parent.
		candidateParent.Pieces.Range(func(key, value any) bool {
			candidateParentPiece, ok := value.(*resource.Piece)
			if !ok {
				candidateParent.Log.Error("invalid piece %s %#v", key, value)
				return true
			}

			piece := &commonv2.Piece{
				Number:      candidateParentPiece.Number,
				ParentId:    candidateParentPiece.ParentID,
				Offset:      candidateParentPiece.Offset,
				Length:      candidateParentPiece.Length,
				TrafficType: candidateParentPiece.TrafficType,
				Cost:        durationpb.New(candidateParentPiece.Cost),
				CreatedAt:   timestamppb.New(candidateParentPiece.CreatedAt),
			}

			if candidateParentPiece.Digest != nil {
				piece.Digest = candidateParentPiece.Digest.String()
			}

			parent.Pieces = append(parent.Pieces, piece)
			return true
		})

		// Set task to parent.
		parent.Task = &commonv2.Task{
			Id:            candidateParent.Task.ID,
			Type:          candidateParent.Task.Type,
			Url:           candidateParent.Task.URL,
			Tag:           candidateParent.Task.Tag,
			Application:   candidateParent.Task.Application,
			Filters:       candidateParent.Task.Filters,
			Header:        candidateParent.Task.Header,
			PieceLength:   candidateParent.Task.PieceLength,
			ContentLength: candidateParent.Task.ContentLength.Load(),
			PieceCount:    candidateParent.Task.TotalPieceCount.Load(),
			SizeScope:     candidateParent.Task.SizeScope(),
			State:         candidateParent.Task.FSM.Current(),
			PeerCount:     int32(candidateParent.Task.PeerCount()),
			CreatedAt:     timestamppb.New(candidateParent.Task.CreatedAt.Load()),
			UpdatedAt:     timestamppb.New(candidateParent.Task.UpdatedAt.Load()),
		}

		// Set digest to parent task.
		if candidateParent.Task.Digest != nil {
			parent.Task.Digest = candidateParent.Task.Digest.String()
		}

		// Set pieces to parent task.
		candidateParent.Task.Pieces.Range(func(key, value any) bool {
			taskPiece, ok := value.(*resource.Piece)
			if !ok {
				candidateParent.Task.Log.Error("invalid piece %s %#v", key, value)
				return true
			}

			piece := &commonv2.Piece{
				Number:      taskPiece.Number,
				ParentId:    taskPiece.ParentID,
				Offset:      taskPiece.Offset,
				Length:      taskPiece.Length,
				TrafficType: taskPiece.TrafficType,
				Cost:        durationpb.New(taskPiece.Cost),
				CreatedAt:   timestamppb.New(taskPiece.CreatedAt),
			}

			if taskPiece.Digest != nil {
				piece.Digest = taskPiece.Digest.String()
			}

			parent.Task.Pieces = append(parent.Task.Pieces, piece)
			return true
		})

		// Set host to parent.
		parent.Host = &commonv2.Host{
			Id:           candidateParent.Host.ID,
			Type:         uint32(candidateParent.Host.Type),
			Ip:           candidateParent.Host.IP,
			Hostname:     candidateParent.Host.Hostname,
			Port:         candidateParent.Host.Port,
			DownloadPort: candidateParent.Host.DownloadPort,
		}

		parents = append(parents, parent)
	}

	return &schedulerv2.AnnouncePeerResponse_NormalTaskResponse{
		NormalTaskResponse: &schedulerv2.NormalTaskResponse{
			CandidateParents:     parents,
			ConcurrentPieceCount: int32(concurrentPieceCount),
		},
	}
}

// constructSuccessPeerPacket constructs peer successful packet.
// Used only in v1 version of the grpc.
func constructSuccessPeerPacket(dynconfig config.DynconfigInterface, peer *resource.Peer, parent *resource.Peer, candidateParents []*resource.Peer) *schedulerv1.PeerPacket {
	concurrentPieceCount := config.DefaultPeerConcurrentPieceCount
	if config, err := dynconfig.GetSchedulerClusterClientConfig(); err == nil && config.ConcurrentPieceCount > 0 {
		concurrentPieceCount = int(config.ConcurrentPieceCount)
	}

	var parents []*schedulerv1.PeerPacket_DestPeer
	for _, candidateParent := range candidateParents {
		parents = append(parents, &schedulerv1.PeerPacket_DestPeer{
			Ip:      candidateParent.Host.IP,
			RpcPort: candidateParent.Host.Port,
			PeerId:  candidateParent.ID,
		})
	}

	return &schedulerv1.PeerPacket{
		TaskId:        peer.Task.ID,
		SrcPid:        peer.ID,
		ParallelCount: int32(concurrentPieceCount),
		MainPeer: &schedulerv1.PeerPacket_DestPeer{
			Ip:      parent.Host.IP,
			RpcPort: parent.Host.Port,
			PeerId:  parent.ID,
		},
		CandidatePeers: parents,
		Code:           commonv1.Code_Success,
	}
}
