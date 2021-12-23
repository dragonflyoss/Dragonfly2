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

	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Callback interface {
	Reschedule(context.Context, *entity.Peer)
	InitReport(context.Context, *entity.Peer)
	PieceSuccess(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PieceFail(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	PeerSuccess(context.Context, *entity.Peer, *rpcscheduler.PeerResult)
	PeerFail(context.Context, *entity.Peer, *rpcscheduler.PeerResult)
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
