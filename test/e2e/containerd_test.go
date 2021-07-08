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
	"os/exec"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
)

var _ = Describe("Containerd with CRI support", func() {
	Context("docker.io/library/busybox:latest image", func() {
		It("pull should be ok", func() {
			cmd := exec.Command("crictl", "pull", "docker.io/library/busybox:latest")
			_, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
		})

		It("rmi should be ok", func() {
			cmd := exec.Command("crictl", "rmi", "docker.io/library/busybox:latest")
			_, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
		})

		It("pull error image", func() {
			cmd := exec.Command("crictl", "pull", "docker.io/library/foo")
			_, err := cmd.CombinedOutput()
			Expect(err).Should(HaveOccurred())
		})
	})

	Context("measures docker.io/library/busybox:latest image", func() {
		It("10 times", func() {
			experiment := gmeasure.NewExperiment("crictl performance")
			experiment.SampleDuration("runtime", func(idx int) {
				var cmd *exec.Cmd
				var err error

				cmd = exec.Command("crictl", "pull", "docker.io/library/busybox:latest")
				_, err = cmd.CombinedOutput()
				Expect(err).NotTo(HaveOccurred())

				cmd = exec.Command("crictl", "rmi", "docker.io/library/busybox:latest")
				_, err = cmd.CombinedOutput()
				Expect(err).NotTo(HaveOccurred())
			}, gmeasure.SamplingConfig{N: 10})
		})
	})
})
