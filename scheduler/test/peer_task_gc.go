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

package test

import (
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/types"
	"time"
)

func (suite *SchedulerTestSuite) Test201PeerTaskGC() {
	var (
		hostMgr      = mgr.GetHostManager()
		peertaskMgr  = mgr.GetPeerTaskManager()
		oldDelayTime = peertaskMgr.GetGCDelayTime()
	)
	peertaskMgr.SetGCDelayTime(time.Second)
	defer peertaskMgr.SetGCDelayTime(oldDelayTime)
	pid := "gc001"
	host := types.CopyHost(&types.Host{PeerHost: scheduler.PeerHost{Uuid: "gc-host-001"}})
	task := types.CopyTask(&types.Task{TaskId: "gc-task-001"})
	hostMgr.AddHost(host)
	peertaskMgr.AddPeerTask(pid, task, host)
	time.Sleep(time.Second / 2)
	peerTask, _ := peertaskMgr.GetPeerTask(pid)
	if !suite.NotEmpty(peerTask, "peer task deleted before gc"){
		return
	}
	time.Sleep(time.Second)
	peerTask, _ = peertaskMgr.GetPeerTask(pid)
	if !suite.Empty(peerTask, "peer task should be gc by peer task manager"){
		return
	}
	host, _ = hostMgr.GetHost(host.Uuid)
	if !suite.Empty(host, "host should be gc by peer task manager"){
		return
	}
}
