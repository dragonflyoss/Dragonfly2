package test

import (
	"github.com/dragonflyoss/Dragonfly2/scheduler/test/common"
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

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	// Run only on Ginkgo node 1
	return nil

}, func(data []byte) {
	// Run on all Ginkgo nodes
})

var _ = ginkgo.SynchronizedAfterSuite(func() {

}, func() {

})
