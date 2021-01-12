package test

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_cdn"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_client"
	. "github.com/onsi/ginkgo"
	"reflect"
	"time"
)

var _ = FDescribe("Multi Client Download Test", func() {
	tl := common.NewE2ELogger()

	var (
		cdn        *mock_cdn.MockCDN
		clientNum  = 20
		clientList []*mock_client.MockClient
		stopChList []chan struct{}
		svr        = server.NewServer()
		ss         = svr.GetServer()
	)

	Describe("start cdn and scheduler", func() {
		It("start cdn and scheduler", func() {
			cdn = mock_cdn.NewMockCDN("localhost:12345", tl)
			cdn.Start()
			mgr.GetCDNManager().InitCDNClient()
			go svr.Start()
			time.Sleep(time.Second/2)
		})
	})

	Describe("Create Multi Client", func() {
		It("create multi client should be successfully", func() {
			mock_client.ClearClient()
			mock_client.RegisterClient(cdn.GetHostId(), cdn)
			for i := 0; i < clientNum; i++ {
				client := mock_client.NewMockClient("127.0.0.1:8002", tl)
				go client.Start()
				stopCh := client.GetStopChan()
				stopChList = append(stopChList, stopCh)
			}
		})
	})

	Describe("Wait Clients Finish", func() {
		It("all clients should be stopped successfully", func() {
			timer := time.After(time.Minute * 10)
			caseList := []reflect.SelectCase{
				{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer), Send: reflect.Value{}},
			}
			for _, stopCh := range stopChList {
				caseList = append(caseList, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopCh), Send: reflect.Value{}})
			}
			closedNumber := 0
			for {
				selIndex, _, _ := reflect.Select(caseList)
				caseList = append(caseList[:selIndex], caseList[selIndex+1:]...)
				if selIndex == 0 {
					tl.Fatalf("download file failed")
				} else {
					closedNumber++
					if closedNumber >= clientNum {
						break
					}
				}
			}
			tl.Log("all client download file finished")
		})
	})

	Describe("stop cdn and scheduler", func() {
		It("stop cdn and scheduler", func() {
			_ = ss
			_ = clientList
			svr.Stop()
			if cdn != nil {
				cdn.Stop()
			}
		})
	})
})
