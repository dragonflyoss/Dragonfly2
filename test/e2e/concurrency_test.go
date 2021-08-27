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

var _ = Describe("Download concurrency", func() {
	Context("ab", func() {
		It("concurrent 100 should be ok", func() {
			url := e2eutil.GetFileURL(HostnameFilePath)
			fmt.Println("download url " + url)

			out, err := e2eutil.ABCommand("-c", "100", "-n", "200", "-X", Proxy, url).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})

		It("concurrent 200 should be ok", func() {
			url := e2eutil.GetFileURL(HostnameFilePath)
			fmt.Println("download url " + url)

			out, err := e2eutil.ABCommand("-c", "200", "-n", "400", "-X", Proxy, url).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})

		It("concurrent 500 should be ok", func() {
			url := e2eutil.GetFileURL(HostnameFilePath)
			fmt.Println("download url " + url)

			out, err := e2eutil.ABCommand("-c", "500", "-n", "1000", "-X", Proxy, url).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})

		It("concurrent 1000 should be ok", func() {
			url := e2eutil.GetFileURL(HostnameFilePath)
			fmt.Println("download url " + url)

			out, err := e2eutil.ABCommand("-c", "1000", "-n", "2000", "-X", Proxy, url).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
