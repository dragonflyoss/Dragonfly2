package test

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_cdn"
	"testing"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

type tester struct {
	start time.Time
	t     *testing.T
}

func (t *tester) Fail() {
	t.t.Logf("--- FAIL: TestE2E(%f)", time.Since(t.start).Round(1*time.Millisecond).Seconds())
}

// RunE2ETests checks configuration parameters (specified through flags) and then runs
// E2E tests using the Ginkgo runner.
// This function is called on each Ginkgo node in parallel mode.
func RunE2ETests(t *testing.T) {
	gomega.RegisterFailHandler(common.Fail)
	ginkgo.RunSpecs(&tester{time.Now(), t}, "Scheduler e2e suite")
}

var (
	cdn        *mock_cdn.MockCDN
	svr    *server.Server
	ss     *server.SchedulerServer
)

var _ = ginkgo.BeforeSuite(func(){
	cdn = mock_cdn.NewMockCDN("localhost:12345", common.NewE2ELogger())
	cdn.Start()
	time.Sleep(time.Second/2)
	mgr.GetCDNManager().InitCDNClient()
	svr        = server.NewServer()
	ss         = svr.GetServer()
	go svr.Start()
	time.Sleep(time.Second/2)
})

var _ = ginkgo.AfterSuite(func(){
	svr.Stop()
	if cdn != nil {
		cdn.Stop()
	}
})
