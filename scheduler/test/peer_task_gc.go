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
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/types"
	. "github.com/onsi/ginkgo"
	"time"
)

var _ = Describe("PeerTask GC Test", func() {
	tl := common.NewE2ELogger()

	var (
		hostMgr      = mgr.GetHostManager()
		peertaskMgr  = mgr.GetPeerTaskManager()
		oldDelayTime = peertaskMgr.GetGCDelayTime()
	)

	Describe("set peer task gc delay time", func() {
		It("set peer task gc delay time", func() {
			peertaskMgr.SetGCDelayTime(time.Second)
		})
	})

	Describe("peer task should be removed by gc", func() {
		It("peer task should be removed by gc", func() {
			pid := "gc001"
			host := types.CopyHost(&types.Host{PeerHost: scheduler.PeerHost{Uuid: "gc-host-001"}})
			task := types.CopyTask(&types.Task{TaskId: "gc-task-001"})
			hostMgr.AddHost(host)
			peertaskMgr.AddPeerTask(pid, task, host)
			time.Sleep(time.Second / 2)
			peerTask, _ := peertaskMgr.GetPeerTask(pid)
			if peerTask == nil {
				tl.Fatalf("peer task deleted before gc")
			}
			time.Sleep(time.Second)
			peerTask, _ = peertaskMgr.GetPeerTask(pid)
			if peerTask != nil {
				tl.Fatalf("peer task should be gc by peer task manager")
			}
			host, _ = hostMgr.GetHost(host.Uuid)
			if host != nil {
				tl.Fatalf("host should be gc by peer task manager")
			}
		})
	})

	Describe("set back peer task gc delay time", func() {
		It("set back peer task gc delay time", func() {
			peertaskMgr.SetGCDelayTime(oldDelayTime)
		})
	})
})
