/*
 *     Copyright 2023 The Dragonfly Authors
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

package rpcserver

import (
	"context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource/persistentcache"
	"d7y.io/dragonfly/v2/scheduler/resource/standard"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/service"
)

// schedulerServerV2 is v2 version of the scheduler grpc server.
type schedulerServerV2 struct {
	// Service interface.
	service *service.V2
}

// newSchedulerServerV2 returns v2 version of the scheduler server.
func newSchedulerServerV2(
	cfg *config.Config,
	resource standard.Resource,
	persistentCacheResource persistentcache.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
) schedulerv2.SchedulerServer {
	return &schedulerServerV2{service.NewV2(cfg, resource, persistentCacheResource, scheduling, dynconfig)}
}

// AnnouncePeer announces peer to scheduler.
func (s *schedulerServerV2) AnnouncePeer(stream schedulerv2.Scheduler_AnnouncePeerServer) error {
	// Collect ConcurrentScheduleGauge metrics.
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	// Collect AnnouncePeerCount metrics.
	metrics.AnnouncePeerCount.Inc()
	if err := s.service.AnnouncePeer(stream); err != nil {
		// Collect AnnouncePeerFailureCount metrics.
		metrics.AnnouncePeerFailureCount.Inc()
		return err
	}

	return nil
}

// StatPeer checks information of peer.
func (s *schedulerServerV2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest) (*commonv2.Peer, error) {
	// Collect StatPeerCount metrics.
	metrics.StatPeerCount.Inc()
	resp, err := s.service.StatPeer(ctx, req)
	if err != nil {
		// Collect StatPeerFailureCount metrics.
		metrics.StatPeerFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// DeletePeer releases peer in scheduler.
func (s *schedulerServerV2) DeletePeer(ctx context.Context, req *schedulerv2.DeletePeerRequest) (*emptypb.Empty, error) {
	// Collect LeavePeerCount metrics.
	metrics.LeavePeerCount.Inc()
	if err := s.service.DeletePeer(ctx, req); err != nil {
		// Collect LeavePeerFailureCount metrics.
		metrics.LeavePeerFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// StatTask checks information of task.
func (s *schedulerServerV2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
	// Collect StatTaskCount metrics.
	metrics.StatTaskCount.Inc()
	resp, err := s.service.StatTask(ctx, req)
	if err != nil {
		// Collect StatTaskFailureCount metrics.
		metrics.StatTaskFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// DeleteTask releases task in scheduler.
func (s *schedulerServerV2) DeleteTask(ctx context.Context, req *schedulerv2.DeleteTaskRequest) (*emptypb.Empty, error) {
	// Collect LeaseTaskCount metrics.
	metrics.LeaveTaskCount.Inc()
	if err := s.service.DeleteTask(ctx, req); err != nil {
		// Collect LeaseTaskFailureCount metrics.
		metrics.LeaveTaskFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// AnnounceHost announces host to scheduler.
func (s *schedulerServerV2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) (*emptypb.Empty, error) {
	// Collect AnnounceHostCount metrics.
	metrics.AnnounceHostCount.WithLabelValues(req.Host.GetOs(), req.Host.GetPlatform(), req.Host.GetPlatformFamily(), req.Host.GetPlatformFamily(),
		req.Host.GetKernelVersion(), req.Host.Build.GetGitVersion(), req.Host.Build.GetGitCommit(), req.Host.Build.GetGoVersion(), req.Host.Build.GetPlatform()).Inc()
	if err := s.service.AnnounceHost(ctx, req); err != nil {
		// Collect AnnounceHostFailureCount metrics.
		metrics.AnnounceHostFailureCount.WithLabelValues(req.Host.GetOs(), req.Host.GetPlatform(), req.Host.GetPlatformFamily(), req.Host.GetPlatformFamily(),
			req.Host.GetKernelVersion(), req.Host.Build.GetGitVersion(), req.Host.Build.GetGitCommit(), req.Host.Build.GetGoVersion(), req.Host.Build.GetPlatform()).Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// ListHosts lists hosts in scheduler.
func (s *schedulerServerV2) ListHosts(ctx context.Context, _ *emptypb.Empty) (*schedulerv2.ListHostsResponse, error) {
	// Collect ListHostsCount metrics.
	metrics.ListHostsCount.Inc()
	resp, err := s.service.ListHosts(ctx)
	if err != nil {
		// Collect ListHostsFailureCount metrics.
		metrics.ListHostsCountFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// DeleteHost releases host in scheduler.
func (s *schedulerServerV2) DeleteHost(ctx context.Context, req *schedulerv2.DeleteHostRequest) (*emptypb.Empty, error) {
	// Collect LeaveHostCount metrics.
	metrics.LeaveHostCount.Inc()
	if err := s.service.DeleteHost(ctx, req); err != nil {
		// Collect LeaveHostFailureCount metrics.
		metrics.LeaveHostFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// AnnouncePersistentCachePeer announces persistent cache peer to scheduler.
func (s *schedulerServerV2) AnnouncePersistentCachePeer(stream schedulerv2.Scheduler_AnnouncePersistentCachePeerServer) error {
	// Collect AnnouncePersistentCachePeerCount metrics.
	metrics.AnnouncePersistentCachePeerCount.Inc()
	if err := s.service.AnnouncePersistentCachePeer(stream); err != nil {
		// Collect AnnouncePersistentCachePeerFailureCount metrics.
		metrics.AnnouncePersistentCachePeerFailureCount.Inc()
		return err
	}

	return nil
}

// StatPersistentCachePeer checks information of persistent cache peer.
func (s *schedulerServerV2) StatPersistentCachePeer(ctx context.Context, req *schedulerv2.StatPersistentCachePeerRequest) (*commonv2.PersistentCachePeer, error) {
	// Collect StatPersistentCachePeerCount metrics.
	metrics.StatPersistentCachePeerCount.Inc()
	resp, err := s.service.StatPersistentCachePeer(ctx, req)
	if err != nil {
		// Collect StatPersistentCachePeerFailureCount metrics.
		metrics.StatPersistentCachePeerFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// DeletePersistentCachePeer releases persistent cache peer in scheduler.
func (s *schedulerServerV2) DeletePersistentCachePeer(ctx context.Context, req *schedulerv2.DeletePersistentCachePeerRequest) (*emptypb.Empty, error) {
	// Collect DeletePersistentCachePeerCount metrics.
	metrics.DeletePersistentCachePeerCount.Inc()
	if err := s.service.DeletePersistentCachePeer(ctx, req); err != nil {
		// Collect DeletePersistentCachePeerFailureCount metrics.
		metrics.DeletePersistentCachePeerFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// UploadPersistentCacheTaskStarted uploads the metadata of the persistent cache task started.
func (s *schedulerServerV2) UploadPersistentCacheTaskStarted(ctx context.Context, req *schedulerv2.UploadPersistentCacheTaskStartedRequest) (*emptypb.Empty, error) {
	// Collect UploadPersistentCacheTaskStartedCount metrics.
	metrics.UploadPersistentCacheTaskStartedCount.Inc()
	if err := s.service.UploadPersistentCacheTaskStarted(ctx, req); err != nil {
		// Collect UploadPersistentCacheTaskStartedFailureCount metrics.
		metrics.UploadPersistentCacheTaskStartedFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// UploadPersistentCacheTaskFinished uploads the metadata of the persistent cache task finished.
func (s *schedulerServerV2) UploadPersistentCacheTaskFinished(ctx context.Context, req *schedulerv2.UploadPersistentCacheTaskFinishedRequest) (*commonv2.PersistentCacheTask, error) {
	// Collect UploadPersistentCacheTaskFinishedCount metrics.
	metrics.UploadPersistentCacheTaskFinishedCount.Inc()
	resp, err := s.service.UploadPersistentCacheTaskFinished(ctx, req)
	if err != nil {
		// Collect UploadPersistentCacheTaskFinishedFailureCount metrics.
		metrics.UploadPersistentCacheTaskFinishedFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// UploadPersistentCacheTaskFailed uploads the metadata of the persistent cache task failed.
func (s *schedulerServerV2) UploadPersistentCacheTaskFailed(ctx context.Context, req *schedulerv2.UploadPersistentCacheTaskFailedRequest) (*emptypb.Empty, error) {
	// Collect UploadPersistentCacheTaskFailedCount metrics.
	metrics.UploadPersistentCacheTaskFailedCount.Inc()
	if err := s.service.UploadPersistentCacheTaskFailed(ctx, req); err != nil {
		// Collect UploadPersistentCacheTaskFailedFailureCount metrics.
		metrics.UploadPersistentCacheTaskFailedFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// StatPersistentCacheTask checks information of persistent cache task.
func (s *schedulerServerV2) StatPersistentCacheTask(ctx context.Context, req *schedulerv2.StatPersistentCacheTaskRequest) (*commonv2.PersistentCacheTask, error) {
	// Collect StatPersistentCacheTaskCount metrics.
	metrics.StatPersistentCacheTaskCount.Inc()
	resp, err := s.service.StatPersistentCacheTask(ctx, req)
	if err != nil {
		// Collect StatPersistentCacheTaskFailureCount metrics.
		metrics.StatPersistentCacheTaskFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// DeletePersistentCacheTask releases persistent cache task in scheduler.
func (s *schedulerServerV2) DeletePersistentCacheTask(ctx context.Context, req *schedulerv2.DeletePersistentCacheTaskRequest) (*emptypb.Empty, error) {
	// Collect DeletePersistentCacheTaskCount metrics.
	metrics.DeletePersistentCacheTaskCount.Inc()
	if err := s.service.DeletePersistentCacheTask(ctx, req); err != nil {
		// Collect DeletePersistentCacheTaskFailureCount metrics.
		metrics.DeletePersistentCacheTaskFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}
