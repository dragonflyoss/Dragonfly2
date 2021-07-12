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

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

const (
	dragonflyNamespace    = "dragonfly-system"
	dragonflyE2ENamespace = "dragonfly-e2e"
)

var _ = Describe("Download with dfget", func() {
	Context("dfget", func() {
		It("setup file server should be ok", func() {
			// create file server
			out, err := e2eutil.KubeCtlCommand("apply", "-f", "../testdata/k8s/file-server.yaml").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			// wait file server ready
			out, err = e2eutil.KubeCtlCommand("-n", dragonflyE2ENamespace,
				"wait", "--for=condition=ready", "--timeout=5m", "pod", "file-server-0").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))
		})

		It("dfget download should be ok", func() {
			out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=dfdaemon",
				"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
			podName := strings.Trim(string(out), "'")
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(podName)
			Expect(strings.HasPrefix(podName, "dragonfly-dfdaemon-")).Should(BeTrue())
			pod := e2eutil.NewPodExec(dragonflyNamespace, podName)

			files := []string{
				"/etc/containerd/config.toml",
				"/etc/fstab",
				"/etc/hostname",
				"/usr/bin/kubectl",
				"/usr/bin/systemctl",
				"/usr/local/bin/containerd-shim",
				"/usr/local/bin/clean-install",
				"/usr/local/bin/entrypoint",
				"/usr/local/bin/containerd-shim-runc-v2",
				"/usr/local/bin/ctr",
				"/usr/local/bin/containerd",
				"/usr/local/bin/create-kubelet-cgroup-v2",
				"/usr/local/bin/crictl",
				"/usr/local/bin/containerd-fuse-overlayfs-grpc",
			}
			for i := range files {
				url := fmt.Sprintf("http://file-server.dragonfly-e2e.svc/kind%s", files[i])
				fmt.Println("download url " + url)

				// get original file digest
				out, err = e2eutil.DockerCommand("sha256sum", files[i]).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum1 := strings.Split(string(out), " ")[0]

				// download file
				out, err = pod.Command("dfget", "-O", "/tmp/d7y.out", url).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())

				// get downloaded file digest
				out, err = pod.Command("sha256sum", "/tmp/d7y.out").CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum2 := strings.Split(string(out), " ")[0]

				Expect(sha256sum1).To(Equal(sha256sum2))
			}
		})
	})
})
