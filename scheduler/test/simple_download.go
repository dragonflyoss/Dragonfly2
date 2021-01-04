package test

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_cdn"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_client"
	. "github.com/onsi/ginkgo"
	"time"
)

var _ = Describe("One Client Download Test", func() {
	tl := common.NewE2ELogger()

	var (
		cdn    *mock_cdn.MockCDN
		client *mock_client.MockClient
		svr    = server.NewServer()
		ss     = svr.GetServer()
	)

	Describe("start cdn and scheduler", func() {
		It("start cdn and scheduler", func() {
			cdn = mock_cdn.NewMockCDN(":12345", tl)
			cdn.Start()
			mgr.GetCDNManager().InitCDNClient()
			go svr.Start()
		})
	})

	Describe("One Client Download a file Test", func() {
		It("should be download a file successfully", func() {
			client = mock_client.NewMockClient("127.0.0.1:8002", tl)
			go client.Start()
			stopCh := client.GetStopChan()
			select {
			case <-stopCh:
				tl.Log("client download file finished")
			case <-time.After(time.Minute):
				tl.Fatalf("download file failed")
			}
		})
	})

	Describe("stop cdn and scheduler", func() {
		It("stop cdn and scheduler", func() {
			_ = ss
			svr.Stop()
			if cdn != nil {
				cdn.Stop()
			}
		})
	})
})
