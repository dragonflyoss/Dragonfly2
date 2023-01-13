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

package service

import (
	"context"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// TODO Implement v2 version of the service functions.
// V2 is the interface for v2 version of the service.
type V2 struct {
	// Resource interface.
	resource resource.Resource

	// Scheduler interface.
	scheduler scheduler.Scheduler

	// Scheduelr service config.
	config *config.Config

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Storage interface.
	storage storage.Storage
}

// New v2 version of service instance.
func NewV2(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
) *V2 {
	return &V2{
		resource:  resource,
		scheduler: scheduler,
		config:    cfg,
		dynconfig: dynconfig,
		storage:   storage,
	}
}

// AnnouncePeer announces peer to scheduler.
func (v *V2) AnnouncePeer(stream schedulerv2.Scheduler_AnnouncePeerServer) error {
	return nil
}

// Checks information of peer.
func (v *V2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest) (*commonv2.Peer, error) {
	return nil, nil
}

// LeavePeer releases peer in scheduler.
func (v *V2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest) error {
	return nil
}

// TODO exchange peer api definition.
// ExchangePeer exchanges peer information.
func (v *V2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest) (*schedulerv2.ExchangePeerResponse, error) {
	return nil, nil
}

// Checks information of task.
func (v *V2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
	return nil, nil
}

// AnnounceHost announces host to scheduler.
func (v *V2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) error {
	return nil
}

// LeaveHost releases host in scheduler.
func (v *V2) LeaveHost(ctx context.Context, req *schedulerv2.LeaveHostRequest) error {
	return nil
}
