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
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	_ "d7y.io/dragonfly/v2/test/e2e/manager"
	"d7y.io/dragonfly/v2/test/e2e/util"
)

var featureGatesFlag string

func init() {
	flag.StringVar(&featureGatesFlag, "feature-gates", "", "e2e test feature gates")
}

var _ = AfterSuite(func() {
	for _, server := range servers {
		for i := 0; i < server.replicas; i++ {
			out, err := util.KubeCtlCommand("-n", server.namespace, "get", "pod", "-l", fmt.Sprintf("component=%s", server.name),
				"-o", fmt.Sprintf("jsonpath='{.items[%d].metadata.name}'", i)).CombinedOutput()
			if err != nil {
				fmt.Printf("get pod error: %s, output: %s\n", err, string(out))
				continue
			}
			podName := strings.Trim(string(out), "'")
			pod := util.NewPodExec(server.namespace, podName, server.name)

			var (
				retryCount    = 0
				maxRetryCount = 10
			)
		retry:
			countOut, err := util.KubeCtlCommand("-n", server.namespace, "get", "pod", "-l", fmt.Sprintf("component=%s", server.name),
				"-o", fmt.Sprintf("jsonpath='{.items[%d].status.containerStatuses[0].restartCount}'", i)).CombinedOutput()
			if err != nil {
				fmt.Printf("get pod %s restart count error: %s, output: %s\n", podName, err, string(countOut))
				continue
			}
			rawCount := strings.Trim(string(countOut), "'")
			count, err := strconv.Atoi(rawCount)
			if err != nil {
				fmt.Printf("atoi error: %s\n", err)
				continue
			}
			fmt.Printf("pod %s restart count: %d\n", podName, count)

			out, err = pod.Command("sh", "-c", fmt.Sprintf(`
              set -x
              cp -r /var/log/dragonfly/%s /tmp/artifact/%s-%d
              cp -r /var/log/dragonfly/dfget /tmp/artifact/%s-%d-dfget
              find /tmp/artifact -type d -exec chmod 777 {} \;
              `, server.logDirName, server.name, i, server.name, i)).CombinedOutput()
			if err != nil {
				fmt.Printf("copy log output: %q, error: %s, retry after 10s\n", string(out), err)
				// pod maybe restart due to panic, wait it running again
				time.Sleep(10 * time.Second)
				if retryCount < maxRetryCount {
					retryCount++
					goto retry
				}
			}

			if count > 0 {
				if err := util.UploadArtifactPrevStdout(server.namespace, podName, fmt.Sprintf("%s-%d", server.name, i), server.name); err != nil {
					fmt.Printf("upload pod %s artifact stdout file error: %v\n", podName, err)
				}
			}

			if err := util.UploadArtifactStdout(server.namespace, podName, fmt.Sprintf("%s-%d", server.name, i), server.name); err != nil {
				fmt.Printf("upload pod %s artifact prev stdout file error: %v\n", podName, err)
			}

			if server.pprofPort > 0 {
				if out, err := util.UploadArtifactPProf(server.namespace, podName,
					fmt.Sprintf("%s-%d", server.name, i), server.name, server.pprofPort); err != nil {
					fmt.Printf("upload pod %s artifact pprof error: %v, output: %s\n", podName, err, out)
				}
			}

		}
	}
})

var _ = BeforeSuite(func() {
	err := util.FeatureGates.Set(featureGatesFlag)
	Expect(err).NotTo(HaveOccurred())
	fmt.Printf("feature gates: %q, flags: %q\n", util.FeatureGates.String(), featureGatesFlag)

	mode := os.Getenv("DRAGONFLY_COMPATIBILITY_E2E_TEST_MODE")
	imageName := os.Getenv("DRAGONFLY_COMPATIBILITY_E2E_TEST_IMAGE")
	if mode != "" && imageName != "" {
		rawImages, err := util.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", fmt.Sprintf("component=%s", mode),
			"-o", "jsonpath='{range .items[0]}{.spec.containers[0].image}{end}'").CombinedOutput()
		image := strings.Trim(string(rawImages), "'")
		Expect(err).NotTo(HaveOccurred())
		fmt.Printf("special image name: %s\n", image)

		stableImageTag := os.Getenv("DRAGONFLY_STABLE_IMAGE_TAG")
		Expect(fmt.Sprintf("dragonflyoss/%s:%s", imageName, stableImageTag)).To(Equal(image))
	}

	rawGitCommit, err := util.GitCommand("rev-parse", "--short", "HEAD").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	gitCommit := strings.Fields(string(rawGitCommit))[0]
	fmt.Printf("git merge commit: %s\n", gitCommit)

	rawPodName, err := util.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=dfdaemon",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(rawPodName), "'")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.HasPrefix(podName, "dragonfly-dfdaemon-")).Should(BeTrue())

	pod := util.NewPodExec(dragonflyNamespace, podName, "dfdaemon")
	rawDfgetVersion, err := pod.Command("dfget", "version").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	dfgetGitCommit := strings.Fields(string(rawDfgetVersion))[7]
	fmt.Printf("raw dfget version:\n%s\n", rawDfgetVersion)
	fmt.Printf("dfget merge commit: %s\n", dfgetGitCommit)

	if !util.FeatureGates.Enabled(util.FeatureGateCommit) {
		return
	}
	if mode == dfdaemonCompatibilityTestMode {
		Expect(gitCommit).NotTo(Equal(dfgetGitCommit))
		return
	}

	Expect(gitCommit).To(Equal(dfgetGitCommit))

	if util.FeatureGates.Enabled(util.FeatureGateRange) {
		out, err := util.DockerCopy("/bin/", "/tmp/sha256sum-offset").CombinedOutput()
		if err != nil {
			fmt.Println(string(out))
		}
		Expect(err).NotTo(HaveOccurred())
	}

	if util.FeatureGates.Enabled(util.FeatureGateRecursive) {
		out, err := util.DockerCopy("/bin/", "/tmp/download-grpc-test").CombinedOutput()
		if err != nil {
			fmt.Println(string(out))
		}
		Expect(err).NotTo(HaveOccurred())
	}
})

// TestE2E is the root of e2e test function
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dragonfly e2e test suite")
}
