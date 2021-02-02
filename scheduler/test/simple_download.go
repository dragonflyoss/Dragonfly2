package test

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_client"
	. "github.com/onsi/ginkgo"
	"time"
)

var _ = Describe("One Client Download Test", func() {
	tl := common.NewE2ELogger()

	var (
		client *mock_client.MockClient
	)

	Describe("One Client Download a file Test", func() {
		It("should be download a file successfully", func() {
			client = mock_client.NewMockClient("127.0.0.1:8002", "http://dragonfly.com?type=single", "s", tl)
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
})
