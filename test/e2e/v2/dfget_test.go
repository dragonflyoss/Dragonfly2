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

	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/test/e2e/v2/util"
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

func getFileSizes() map[string]int {
	var (
		details = map[string]int{}
		files   = util.GetFileList()
	)

	fmt.Printf("dfget-empty-file feature gate enabled\n")
	files = append(files, "/tmp/empty-file")
	for _, path := range files {
		out, err := util.DockerCommand("stat", "--printf=%s", path).CombinedOutput()
		if err != nil {
			fmt.Printf("stat %s erro: %s, stdout: %s", path, err, string(out))
		}
		Expect(err).NotTo(HaveOccurred())
		size, err := strconv.Atoi(string(out))
		Expect(err).NotTo(HaveOccurred())
		details[path] = size
	}
	return details
}

func getRandomRange(size int) *http.Range {
	if size == 0 {
		return &http.Range{
			Start:  0,
			Length: 0,
		}
	}
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
	rg := &http.Range{
		Start:  int64(start),
		Length: int64(end + 1 - start),
	}
	return rg
}

func singleDfgetTest(name, ns, label, podNamePrefix, container string) {
	It(name, Label("download", "normal"), func() {
		fileDetails := getFileSizes()
		out, err := util.KubeCtlCommand("-n", ns, "get", "pod", "-l", label,
			"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
		podName := strings.Trim(string(out), "'")
		Expect(err).NotTo(HaveOccurred())
		fmt.Println("test in pod: " + podName)
		Expect(strings.HasPrefix(podName, podNamePrefix)).Should(BeTrue())

		// copy test tools into container
		out, err = util.KubeCtlCommand("-n", ns, "cp", "-c", container, "/tmp/sha256sum-offset",
			fmt.Sprintf("%s:/bin/", podName)).CombinedOutput()
		if err != nil {
			fmt.Println(string(out))
		}
		Expect(err).NotTo(HaveOccurred())

		pod := util.NewPodExec(ns, podName, container)

		// install curl
		out, err = pod.Command("apk", "add", "-U", "curl").CombinedOutput()
		fmt.Println("apk output: " + string(out))
		Expect(err).NotTo(HaveOccurred())

		for path, size := range fileDetails {
			// skip empty file
			if size == 0 {
				continue
			}
			url1 := util.GetFileURL(path)
			url2 := util.GetNoContentLengthFileURL(path)

			// make ranged requests to invoke prefetch feature
			rg1, rg2 := getRandomRange(size), getRandomRange(size)
			downloadSingleFile(ns, pod, path, url1, size, rg1, rg1.String())
			downloadSingleFile(ns, pod, path, url2, size, rg2, rg2.String())

			rg3, rg4 := getRandomRange(size), getRandomRange(size)
			// set target length
			rg3.Length = int64(size) - rg3.Start
			rg4.Length = int64(size) - rg4.Start

			downloadSingleFile(ns, pod, path, url1, size, rg3, fmt.Sprintf("bytes=%d-", rg3.Start))
		}
	})
}

func downloadSingleFile(ns string, pod *util.PodExec, path, url string, size int, rg *http.Range, rawRg string) {
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
			fmt.Sprintf("Range: %s", rawRg), url)
		curl = append(curl, "/usr/bin/curl", "-x", "http://127.0.0.1:65001", "-s", "--dump-header", "-", "-o", "/tmp/curl.out",
			"--header", fmt.Sprintf("Range: %s", rawRg), url)

		sha256sumOffset = append(sha256sumOffset, "sh", "-c",
			fmt.Sprintf("/bin/sha256sum-offset -file %s -offset %d -length %d",
				"/var/lib/dragonfly/d7y.offset.out", rg.Start, rg.Length))
		dfgetOffset = append(dfgetOffset, "/opt/dragonfly/bin/dfget", "--disable-back-source", "--original-offset", "-O", "/var/lib/dragonfly/d7y.offset.out", "-H",
			fmt.Sprintf("Range: %s", rawRg), url)
	}

	fmt.Printf("--------------------------------------------------------------------------------\n\n")
	if rg == nil {
		fmt.Printf("download %s, size %d\n", url, size)
	} else {
		fmt.Printf("download %s, size %d, request range: %s, target length: %d\n",
			url, size, rawRg, rg.Length)
	}
	// get original file digest
	out, err := util.DockerCommand(sha256sum...).CombinedOutput()
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
	Expect(end.Sub(start).Seconds() < 50.0).To(Equal(true))

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
		Expect(end.Sub(start).Seconds() < 50.0).To(Equal(true))
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
	Expect(end.Sub(start).Seconds() < 50.0).To(Equal(true))
}
