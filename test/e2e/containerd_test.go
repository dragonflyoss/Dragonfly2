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
	"context"
	"fmt"

	"github.com/containerd/containerd"

	//nolint
	. "github.com/onsi/ginkgo"

	//nolint
	. "github.com/onsi/gomega"
)

var _ = Describe("Containerd", func() {
	Context("Pull docker.io/library/busybox image", func() {
		It("should be ok", func() {
			image, err := cdClient.Pull(context.Background(), "docker.io/library/busybox", containerd.WithPullUnpack)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(image)
		})
	})

})
