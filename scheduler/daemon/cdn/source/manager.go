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

package source

import (
	"context"

	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	managerRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types/host"
	"d7y.io/dragonfly/v2/scheduler/types/task"
)

type manager struct {
	taskManager daemon.TaskMgr
	hostManager daemon.HostMgr
	peerManager daemon.PeerMgr
}

func (m manager) OnNotify(scheduler *managerRPC.Scheduler) {
	panic("implement me")
}

func (m manager) StartSeedTask(ctx context.Context, task *task.Task, callback func(ps *cdnsystem.PieceSeed, err error)) error {
	panic("implement me")
}

func (m manager) DownloadTinyFileContent(task *task.Task, cdnHost *host.NodeHost) ([]byte, error) {
	panic("implement me")
}

func NewManager() daemon.CDNMgr {
	return nil
}

var _ daemon.CDNMgr = (*manager)(nil)
