package test

import (
	"fmt"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/scheduler/mgr"
	"github.com/dragonflyoss/Dragonfly2/scheduler/server"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/mock_cdn"
	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
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
	logger.InitScheduler()
	cdn = mock_cdn.NewMockCDN("localhost:8003", common.NewE2ELogger())
	cdn.Start()
	time.Sleep(time.Second/2)
	mgr.GetCDNManager().InitCDNClient()
	svr        = server.NewServer()
	ss         = svr.GetServer()
	go svr.Start()
	time.Sleep(time.Second/2)
	go func() {
		// enable go pprof and statsview
		// port, _ := freeport.GetFreePort()
		port := 8888
		debugListen := fmt.Sprintf("localhost:%d", port)
		viewer.SetConfiguration(viewer.WithAddr(debugListen))
		logger.With("pprof", fmt.Sprintf("http://%s/debug/pprof", debugListen),
			"statsview", fmt.Sprintf("http://%s/debug/statsview", debugListen)).
			Infof("enable debug at http://%s", debugListen)
		if err := statsview.New().Start(); err != nil {
			logger.Warnf("serve go pprof error: %s", err)
		}
	}()
})

var _ = ginkgo.AfterSuite(func(){
	svr.Stop()
	if cdn != nil {
		cdn.Stop()
	}
})
