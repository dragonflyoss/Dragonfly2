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
	"time"

	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
)

var _ = Describe("Download with dfget and proxy", func() {
	Context("dfget", func() {
		singleDfgetTest("dfget daemon download should be ok",
			dragonflyNamespace, "component=dfdaemon",
			"dragonfly-dfdaemon-", "dfdaemon")
		for i := 0; i < 3; i++ {
			singleDfgetTest(
				fmt.Sprintf("dfget daemon proxy-%d should be ok", i),
				dragonflyE2ENamespace,
				fmt.Sprintf("statefulset.kubernetes.io/pod-name=proxy-%d", i),
				"proxy-", "proxy")
		}
	})
})

func singleDfgetTest(name, ns, label, podNamePrefix, container string) {
	It(name, func() {
		out, err := e2eutil.KubeCtlCommand("-n", ns, "get", "pod", "-l", label,
			"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
		podName := strings.Trim(string(out), "'")
		Expect(err).NotTo(HaveOccurred())
		fmt.Println("test in pod: " + podName)
		Expect(strings.HasPrefix(podName, podNamePrefix)).Should(BeTrue())
		pod := e2eutil.NewPodExec(ns, podName, container)

		var urls = map[string]string{}
		for _, v := range e2eutil.GetFileList() {
			urls[e2eutil.GetFileURL(v)] = v
			urls[e2eutil.GetNoContentLengthFileURL(v)] = v
		}

		for url, path := range urls {
			fmt.Printf("\n--------------------------------------------------------------------------------\n\n")
			fmt.Println("download url: " + url)
			// get original file digest
			out, err = e2eutil.DockerCommand("sha256sum", path).CombinedOutput()
			fmt.Println("original sha256sum: " + string(out))
			Expect(err).NotTo(HaveOccurred())
			sha256sum1 := strings.Split(string(out), " ")[0]

			var (
				start time.Time
				end   time.Time
			)
			// download file via dfget
			start = time.Now()
			out, err = pod.Command("dfget", "-O", "/tmp/d7y.out", url).CombinedOutput()
			end = time.Now()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// get dfget downloaded file digest
			out, err = pod.Command("sha256sum", "/tmp/d7y.out").CombinedOutput()
			fmt.Println("dfget sha256sum: " + string(out))
			Expect(err).NotTo(HaveOccurred())
			sha256sum2 := strings.Split(string(out), " ")[0]
			Expect(sha256sum1).To(Equal(sha256sum2))

			// slow download
			Expect(end.Sub(start).Seconds() < 30.0).To(Equal(true))

			// skip dfdaemon
			if ns == dragonflyNamespace {
				continue
			}
			// download file via proxy
			start = time.Now()
			out, err = pod.Command("sh", "-c", fmt.Sprintf(`
              export http_proxy=http://127.0.0.1:65001
              wget -O /tmp/wget.out %s`, url)).CombinedOutput()
			end = time.Now()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// get proxy downloaded file digest
			out, err = pod.Command("sha256sum", "/tmp/wget.out").CombinedOutput()
			fmt.Println("wget sha256sum: " + string(out))
			Expect(err).NotTo(HaveOccurred())
			sha256sum3 := strings.Split(string(out), " ")[0]
			Expect(sha256sum1).To(Equal(sha256sum3))

			// slow download
			Expect(end.Sub(start).Seconds() < 30.0).To(Equal(true))
		}
	})
}
