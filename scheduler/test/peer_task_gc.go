package test

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
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
			host := types.CopyHost(&types.Host{PeerHost:scheduler.PeerHost{Uuid: "gc-host-001"}})
			task := types.CopyTask(&types.Task{TaskId: "gc-task-001"})
			hostMgr.AddHost(host)
			peertaskMgr.AddPeerTask(pid, task, host)
			time.Sleep(time.Second / 2)
			peerTask, _ := peertaskMgr.GetPeerTask(pid)
			if peerTask == nil {
				tl.Fatalf("peer task deleted before gc")
			}
			time.Sleep(time.Second / 2)
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
