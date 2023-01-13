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

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// TODO Implement v2 version of the rpc server apis.
// schedulerServerV2 is v2 version of the scheduler grpc server.
type schedulerServerV2 struct {
	// Service interface.
	service *service.V2
}

// newSchedulerServerV2 returns v2 version of the scheduler server.
func newSchedulerServerV2(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
) schedulerv2.SchedulerServer {
	return &schedulerServerV2{service.NewV2(cfg, resource, scheduler, dynconfig, storage)}
}

// AnnouncePeer announces peer to scheduler.
func (s *schedulerServerV2) AnnouncePeer(stream schedulerv2.Scheduler_AnnouncePeerServer) error {
	return nil
}

// Checks information of peer.
func (s *schedulerServerV2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest) (*commonv2.Peer, error) {
	return nil, nil
}

// LeavePeer releases peer in scheduler.
func (s *schedulerServerV2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}

// TODO exchange peer api definition.
// ExchangePeer exchanges peer information.
func (s *schedulerServerV2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest) (*schedulerv2.ExchangePeerResponse, error) {
	return nil, nil
}

// Checks information of task.
func (s *schedulerServerV2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
	return nil, nil
}

// AnnounceHost announces host to scheduler.
func (s *schedulerServerV2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) (*emptypb.Empty, error) {
	return nil, nil
}

// LeaveHost releases host in scheduler.
func (s *schedulerServerV2) LeaveHost(ctx context.Context, req *schedulerv2.LeaveHostRequest) (*emptypb.Empty, error) {
	return nil, nil
}
