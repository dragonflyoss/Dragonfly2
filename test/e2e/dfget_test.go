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
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
)

var _ = Describe("Download with dfget and proxy", func() {
	Context("dfget", func() {
		files := getFileSizes()
		singleDfgetTest("dfget daemon download should be ok",
			dragonflyNamespace, "component=dfdaemon",
			"dragonfly-dfdaemon-", "dfdaemon", files)
		for i := 0; i < 3; i++ {
			singleDfgetTest(
				fmt.Sprintf("dfget daemon proxy-%d should be ok", i),
				dragonflyE2ENamespace,
				fmt.Sprintf("statefulset.kubernetes.io/pod-name=proxy-%d", i),
				"proxy-", "proxy", files)
		}
	})
})

func getFileSizes() map[string]int {
	var details = map[string]int{}
	for _, path := range e2eutil.GetFileList() {
		out, err := e2eutil.DockerCommand("stat", "--printf=%s", path).CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		size, err := strconv.Atoi(string(out))
		Expect(err).NotTo(HaveOccurred())
		details[path] = size
	}
	return details
}

func getRandomRange(size int) *util.Range {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	r1 := rnd.Intn(size - 1)
	r2 := rnd.Intn(size - 1)
	var start, end int
	if r1 > r2 {
		start, end = r2, r1
	} else {
		start, end = r1, r2
	}

	// range for [start, end]
	rg := &util.Range{
		Start:  int64(start),
		Length: int64(end + 1 - start),
	}
	return rg
}

func singleDfgetTest(name, ns, label, podNamePrefix, container string, fileDetails map[string]int) {
	It(name, func() {
		out, err := e2eutil.KubeCtlCommand("-n", ns, "get", "pod", "-l", label,
			"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
		podName := strings.Trim(string(out), "'")
		Expect(err).NotTo(HaveOccurred())
		fmt.Println("test in pod: " + podName)
		Expect(strings.HasPrefix(podName, podNamePrefix)).Should(BeTrue())

		// copy test tools into container
		if featureGates.Enabled(featureGateRange) {
			out, err = e2eutil.KubeCtlCommand("-n", ns, "cp", "-c", container, "/tmp/sha256sum-offset",
				fmt.Sprintf("%s:/bin/", podName)).CombinedOutput()
			if err != nil {
				fmt.Println(string(out))
			}
			Expect(err).NotTo(HaveOccurred())
		}

		pod := e2eutil.NewPodExec(ns, podName, container)

		// install curl
		out, err = pod.Command("apk", "add", "-U", "curl").CombinedOutput()
		fmt.Println("apk output: " + string(out))
		Expect(err).NotTo(HaveOccurred())

		for path, size := range fileDetails {
			url1 := e2eutil.GetFileURL(path)
			url2 := e2eutil.GetNoContentLengthFileURL(path)

			// make ranged requests to invoke prefetch feature
			if featureGates.Enabled(featureGateRange) {
				rg1, rg2 := getRandomRange(size), getRandomRange(size)
				downloadSingleFile(ns, pod, path, url1, size, rg1)
				downloadSingleFile(ns, pod, path, url1, size, rg2)
				// FIXME no content length cases are always failed.
				// downloadSingleFile(ns, pod, path, url2, size, rg)
			}

			downloadSingleFile(ns, pod, path, url1, size, nil)

			if featureGates.Enabled(featureGateNoLength) {
				downloadSingleFile(ns, pod, path, url2, size, nil)
			}
		}
	})
}

func downloadSingleFile(ns string, pod *e2eutil.PodExec, path, url string, size int, rg *util.Range) {
	var (
		sha256sum []string
		dfget     []string
		curl      []string

		sha256sumOffset []string
		dfgetOffset     []string
	)

	if rg == nil {
		sha256sum = append(sha256sum, "/usr/bin/sha256sum", path)
		dfget = append(dfget, "/opt/dragonfly/bin/dfget", "--disable-back-source", "-O", "/tmp/d7y.out", url)
		curl = append(curl, "/usr/bin/curl", "-x", "http://127.0.0.1:65001", "-s", "--dump-header", "-", "-o", "/tmp/curl.out", url)
	} else {
		sha256sum = append(sha256sum, "sh", "-c",
			fmt.Sprintf("/bin/sha256sum-offset -file %s -offset %d -length %d", path, rg.Start, rg.Length))
		dfget = append(dfget, "/opt/dragonfly/bin/dfget", "--disable-back-source", "-O", "/tmp/d7y.out", "-H",
			fmt.Sprintf("Range: bytes=%d-%d", rg.Start, rg.Start+rg.Length-1), url)
		curl = append(curl, "/usr/bin/curl", "-x", "http://127.0.0.1:65001", "-s", "--dump-header", "-", "-o", "/tmp/curl.out",
			"--header", fmt.Sprintf("Range: bytes=%d-%d", rg.Start, rg.Start+rg.Length-1), url)

		sha256sumOffset = append(sha256sumOffset, "sh", "-c",
			fmt.Sprintf("/bin/sha256sum-offset -file %s -offset %d -length %d",
				"/var/lib/dragonfly/d7y.offset.out", rg.Start, rg.Length))
		dfgetOffset = append(dfgetOffset, "/opt/dragonfly/bin/dfget", "--disable-back-source", "--original-offset", "-O", "/var/lib/dragonfly/d7y.offset.out", "-H",
			fmt.Sprintf("Range: bytes=%d-%d", rg.Start, rg.Start+rg.Length-1), url)
	}

	fmt.Printf("--------------------------------------------------------------------------------\n\n")
	if rg == nil {
		fmt.Printf("download %s, size %d\n", url, size)
	} else {
		fmt.Printf("download %s, size %d, range: bytes=%d-%d/%d, target length: %d\n",
			url, size, rg.Start, rg.Start+rg.Length-1, size, rg.Length)
	}
	// get original file digest
	out, err := e2eutil.DockerCommand(sha256sum...).CombinedOutput()
	fmt.Println("original sha256sum: " + string(out))
	Expect(err).NotTo(HaveOccurred())
	sha256sum1 := strings.Split(string(out), " ")[0]

	var (
		start time.Time
		end   time.Time
	)
	// download file via dfget
	start = time.Now()
	out, err = pod.Command(dfget...).CombinedOutput()
	end = time.Now()
	fmt.Println(string(out))
	Expect(err).NotTo(HaveOccurred())

	// get dfget downloaded file digest
	out, err = pod.Command("/usr/bin/sha256sum", "/tmp/d7y.out").CombinedOutput()
	fmt.Println("dfget sha256sum: " + string(out))
	Expect(err).NotTo(HaveOccurred())
	sha256sum2 := strings.Split(string(out), " ")[0]
	Expect(sha256sum1).To(Equal(sha256sum2))

	// slow download
	Expect(end.Sub(start).Seconds() < 30.0).To(Equal(true))

	// download file via dfget with offset
	if rg != nil {
		// move output for next cases and debugging
		_, _ = pod.Command("/bin/sh", "-c", `
                rm -f /var/lib/dragonfly/d7y.offset.out.last
                cp -l /var/lib/dragonfly/d7y.offset.out /var/lib/dragonfly/d7y.offset.out.last
                rm -f /var/lib/dragonfly/d7y.offset.out
			`).CombinedOutput()

		start = time.Now()
		out, err = pod.Command(dfgetOffset...).CombinedOutput()
		end = time.Now()
		fmt.Println(string(out))
		Expect(err).NotTo(HaveOccurred())

		// get dfget downloaded file digest
		out, err = pod.Command(sha256sumOffset...).CombinedOutput()
		fmt.Println("dfget with offset sha256sum: " + string(out))
		Expect(err).NotTo(HaveOccurred())
		sha256sumz := strings.Split(string(out), " ")[0]
		Expect(sha256sum1).To(Equal(sha256sumz))

		// slow download
		Expect(end.Sub(start).Seconds() < 30.0).To(Equal(true))
	}

	// skip dfdaemon
	if ns == dragonflyNamespace {
		fmt.Println("skip " + dragonflyNamespace + " namespace proxy tests")
		return
	}
	// download file via proxy
	start = time.Now()
	out, err = pod.Command(curl...).CombinedOutput()
	end = time.Now()
	fmt.Print(string(out))
	Expect(err).NotTo(HaveOccurred())

	// get proxy downloaded file digest
	out, err = pod.Command("/usr/bin/sha256sum", "/tmp/curl.out").CombinedOutput()
	fmt.Println("curl sha256sum: " + string(out))
	Expect(err).NotTo(HaveOccurred())
	sha256sum3 := strings.Split(string(out), " ")[0]
	Expect(sha256sum1).To(Equal(sha256sum3))

	// slow download
	Expect(end.Sub(start).Seconds() < 30.0).To(Equal(true))
}
