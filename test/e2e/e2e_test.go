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

package e2e

import (
	"fmt"
	"strings"
	"testing"

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

var _ = BeforeSuite(func() {
	out, err := e2eutil.GitCommand("rev-parse", "--short", "HEAD").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	gitCommit := strings.Fields(string(out))[0]
	fmt.Println(gitCommit)

	out, err = e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=dfdaemon",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)

	Expect(strings.HasPrefix(podName, "dragonfly-dfdaemon-")).Should(BeTrue())
	pod := e2eutil.NewPodExec(dragonflyNamespace, podName, "dfdaemon")
	out, err = pod.Command("dfget", "version").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	dfgetGitCommit := strings.Fields(string(out))[7]
	fmt.Println(dfgetGitCommit)

	Expect(gitCommit).To(Equal(dfgetGitCommit))
})

var _ = AfterSuite(func() {
	out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=dfdaemon",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.HasPrefix(podName, "dragonfly-dfdaemon-")).Should(BeTrue())

	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, podName, "/var/log/dragonfly/daemon/core.log", "/tmp/artifact/daemon.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-cdn-0", "/var/log/dragonfly/cdn/core.log", "/tmp/artifact/cdn-0.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-cdn-1", "/var/log/dragonfly/cdn/core.log", "/tmp/artifact/cdn-1.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-cdn-2", "/var/log/dragonfly/cdn/core.log", "/tmp/artifact/cdn-2.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-scheduler-0", "/var/log/dragonfly/scheduler/core.log", "/tmp/artifact/scheduler-0.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-scheduler-1", "/var/log/dragonfly/scheduler/core.log", "/tmp/artifact/scheduler-1.log").CombinedOutput()
	e2eutil.KubeCtlCopyCommand(dragonflyNamespace, "dragonfly-scheduler-2", "/var/log/dragonfly/scheduler/core.log", "/tmp/artifact/scheduler-2.log").CombinedOutput()
})

// TestE2E is the root of e2e test function
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dragonfly e2e test suite")
}
