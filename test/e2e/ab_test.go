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

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

var _ = Describe("Download with ab", func() {
	Context("ab", func() {
		It("ab download should be ok", func() {
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
			}

			for i := range files {
				url := fmt.Sprintf("http://file-server.dragonfly-e2e.svc/kind%s", files[i])
				fmt.Println("download url " + url)

				// get original file digest
				out, err := e2eutil.ABCommand("-c", "100", "-n", "1000", "-X", "127.0.0.1:65001", files[i]).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
})
