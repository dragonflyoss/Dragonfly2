/*
 *     Copyright 2024 The Dragonfly Authors
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
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	_ "d7y.io/dragonfly/v2/test/e2e/v2/manager"
	"d7y.io/dragonfly/v2/test/e2e/v2/util"
)

var _ = AfterSuite(func() {
	for _, server := range util.Servers {
		for i := 0; i < server.Replicas; i++ {
			fmt.Printf("\n------------------------------ Get %s-%d Artifact Started ------------------------------\n", server.Name, i)

			out, err := util.KubeCtlCommand("-n", server.Namespace, "get", "pod", "-l", fmt.Sprintf("component=%s", server.Name),
				"-o", fmt.Sprintf("jsonpath='{.items[%d].metadata.name}'", i)).CombinedOutput()
			if err != nil {
				fmt.Printf("get pod error: %s, output: %s\n", err, string(out))
				continue
			}
			podName := strings.Trim(string(out), "'")
			pod := util.NewPodExec(server.Namespace, podName, server.Name)

			countOut, err := util.KubeCtlCommand("-n", server.Namespace, "get", "pod", "-l", fmt.Sprintf("component=%s", server.Name),
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
			find /tmp/artifact -type d -exec chmod 777 {} \;
			`, server.LogDirName, server.Name, i)).CombinedOutput()
			if err != nil {
				fmt.Printf("copy log output: %q, error: %s\n", string(out), err)
			}

			if count > 0 {
				if err := util.UploadArtifactPrevStdout(server.Namespace, podName, fmt.Sprintf("%s-%d", server.Name, i), server.Name); err != nil {
					fmt.Printf("upload pod %s artifact prev stdout file error: %v\n", podName, err)
				}
			}

			if err := util.UploadArtifactStdout(server.Namespace, podName, fmt.Sprintf("%s-%d", server.Name, i), server.Name); err != nil {
				fmt.Printf("upload pod %s artifact stdout file error: %v\n", podName, err)
			}

			fmt.Printf("------------------------------ Get %s-%d Artifact Finished ------------------------------\n", server.Name, i)
		}
	}
})

var _ = BeforeSuite(func() {
	rawGitCommit, err := util.GitCommand("rev-parse", "--short", "HEAD").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	gitCommit := strings.Fields(string(rawGitCommit))[0]
	fmt.Printf("git commit: %s\n", gitCommit)
	// Wait for peers to start and announce to scheduler.
	time.Sleep(5 * time.Minute)
})

// TestE2E is the root of e2e test function
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dragonfly e2e test suite")
}
