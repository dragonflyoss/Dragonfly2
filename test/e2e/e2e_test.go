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
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	_ "d7y.io/dragonfly/v2/test/e2e/manager"
)

const (
	proxy                 = "localhost:65001"
	hostnameFilePath      = "/etc/hostname"
	dragonflyNamespace    = "dragonfly-system"
	dragonflyE2ENamespace = "dragonfly-e2e"
)

const (
	dfdaemonCompatibilityTestMode = "dfdaemon"
)

var _ = BeforeSuite(func() {
	mode := os.Getenv("DRAGONFLY_COMPATIBILITY_E2E_TEST_MODE")
	if mode != "" {
		rawImages, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", fmt.Sprintf("component=%s", mode),
			"-o", "jsonpath='{range .items[0]}{.spec.containers[0].image}{end}'").CombinedOutput()
		image := strings.Trim(string(rawImages), "'")
		Expect(err).NotTo(HaveOccurred())
		fmt.Printf("special image name: %s\n", image)

		stableImageTag := os.Getenv("DRAGONFLY_STABLE_IMAGE_TAG")
		Expect(fmt.Sprintf("dragonflyoss/%s:%s", mode, stableImageTag)).To(Equal(image))
	}

	rawGitCommit, err := e2eutil.GitCommand("rev-parse", "--short", "HEAD").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	gitCommit := strings.Fields(string(rawGitCommit))[0]
	fmt.Printf("git merge commit: %s\n", gitCommit)

	rawPodName, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=dfdaemon",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(rawPodName), "'")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.HasPrefix(podName, "dragonfly-dfdaemon-")).Should(BeTrue())

	pod := e2eutil.NewPodExec(dragonflyNamespace, podName, "dfdaemon")
	rawDfgetVersion, err := pod.Command("dfget", "version").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	dfgetGitCommit := strings.Fields(string(rawDfgetVersion))[7]
	fmt.Printf("dfget merge commit: %s\n", dfgetGitCommit)

	if mode == dfdaemonCompatibilityTestMode {
		Expect(gitCommit).NotTo(Equal(dfgetGitCommit))
		return
	}

	Expect(gitCommit).To(Equal(dfgetGitCommit))
})

// TestE2E is the root of e2e test function
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dragonfly e2e test suite")
}
