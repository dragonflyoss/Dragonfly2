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

package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/internal/idgen"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/structutils"
	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat files should be ok", func() {
			var cdnPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				cdnPods[i] = getCDNExec(i)
			}
			fsPod := getFileServerExec()

			for _, v := range e2eutil.GetFileList() {
				url := e2eutil.GetFileURL(v)
				fmt.Println("download url " + url)

				// get original file digest
				out, err := e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum1 := strings.Split(string(out), " ")[0]

				// preheat file
				req, err := structutils.StructToMap(types.CreatePreheatJobRequest{
					Type: internaljob.PreheatJob,
					Args: types.PreheatArgs{
						Type: "file",
						URL:  url,
					},
					UserID: 1,
				})
				Expect(err).NotTo(HaveOccurred())

				out, err = fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
					fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())

				// wait for success
				job := &model.Job{}
				err = json.Unmarshal(out, job)
				Expect(err).NotTo(HaveOccurred())
				done := waitForDone(job, fsPod)
				Expect(done).Should(BeTrue())

				// generate task_id, also the filename
				cdnTaskID := idgen.TaskID(url, &base.UrlMeta{Tag: managerTag})
				fmt.Println(cdnTaskID)

				sha256sum2 := checkPreheatResult(cdnPods, cdnTaskID)
				if sha256sum2 == "" {
					fmt.Println("preheat file not found")
				}
				Expect(sha256sum1).To(Equal(sha256sum2))
			}
		})

		It("preheat image should be ok", func() {
			url := "https://registry-1.docker.io/v2/library/alpine/manifests/3.14"
			fmt.Println("download image " + url)

			var (
				cdnTaskIDs = []string{
					"effb4ac6e36d9a2a425ab142ba0a21fd0d49feea67a839fbd776ebb04e6f9eb7",
					"ceaaf57ceba7221c2d54c62d77860e28b091837f235ba802c0722c522d6c7a8a",
				}
				sha256sum1 = []string{
					"14119a10abf4669e8cdbdff324a9f9605d99697215a0d21c360fe8dfa8471bab",
					"a0d0a0d46f8b52473982a3c466318f479767577551a53ffc9074c9fa7035982e",
				}
			)

			var cdnPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				cdnPods[i] = getCDNExec(i)
			}
			fsPod := getFileServerExec()

			// preheat file
			req, err := structutils.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "image",
					URL:  url,
				},
				UserID: 1,
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			job := &model.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(job, fsPod)
			Expect(done).Should(BeTrue())

			for i, cdnTaskID := range cdnTaskIDs {
				sha256sum2 := checkPreheatResult(cdnPods, cdnTaskID)
				if sha256sum2 == "" {
					fmt.Println("preheat file not found")
				}
				Expect(sha256sum1[i]).To(Equal(sha256sum2))
			}
		})

		It("concurrency 100 preheat should be ok", func() {
			// generate the data file
			url := e2eutil.GetFileURL(hostnameFilePath)
			fmt.Println("download url " + url)
			dataFilePath := "post_data"
			fd, err := os.Create(dataFilePath)
			Expect(err).NotTo(HaveOccurred())
			_, err = fd.WriteString(fmt.Sprintf(`{"type":"file","url":"%s"}`, url))
			fd.Close()
			Expect(err).NotTo(HaveOccurred())

			// use ab to post the data file to manager concurrently
			out, err := e2eutil.ABCommand("-c", "100", "-n", "200", "-T", "application/json", "-p", dataFilePath, "-X", proxy, fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
			os.Remove(dataFilePath)
			Expect(err).NotTo(HaveOccurred())

			// get original file digest
			out, err = e2eutil.DockerCommand("sha256sum", hostnameFilePath).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
			sha256sum1 := strings.Split(string(out), " ")[0]

			var cdnPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				cdnPods[i] = getCDNExec(i)
			}
			fsPod := getFileServerExec()

			// use a curl to preheat the same file, git a id to wait for success
			req, err := structutils.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  url,
				},
				UserID: 1,
			})

			Expect(err).NotTo(HaveOccurred())
			out, err = fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			job := &model.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(job, fsPod)
			Expect(done).Should(BeTrue())

			// generate task id to find the file
			cdnTaskID := idgen.TaskID(url, &base.UrlMeta{Tag: managerTag})
			fmt.Println(cdnTaskID)

			sha256sum2 := checkPreheatResult(cdnPods, cdnTaskID)
			if sha256sum2 == "" {
				fmt.Println("preheat file not found")
			}
			Expect(sha256sum1).To(Equal(sha256sum2))
		})
	})
})

func waitForDone(preheat *model.Job, pod *e2eutil.PodExec) bool {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			out, err := pod.CurlCommand("", nil, nil,
				fmt.Sprintf("http://%s:%s/%s/%d", managerService, managerPort, preheatPath, preheat.ID)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
			err = json.Unmarshal(out, preheat)
			Expect(err).NotTo(HaveOccurred())
			switch preheat.Status {
			case machineryv1tasks.StateSuccess:
				return true
			case machineryv1tasks.StateFailure:
				return false
			default:
			}
		}
	}
}

func checkPreheatResult(cdnPods [3]*e2eutil.PodExec, cdnTaskID string) string {
	var sha256sum2 string
	for _, cdn := range cdnPods {
		out, err := cdn.Command("ls", cdnCachePath).CombinedOutput()
		if err != nil {
			// if the directory does not exist, skip this cdn
			continue
		}
		// directory name is the first three characters of the task id
		dir := cdnTaskID[0:3]
		if !strings.Contains(string(out), dir) {
			continue
		}

		out, err = cdn.Command("ls", fmt.Sprintf("%s/%s", cdnCachePath, dir)).CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		// file name is the same as task id
		file := cdnTaskID
		if !strings.Contains(string(out), file) {
			continue
		}

		// calculate digest of downloaded file
		out, err = cdn.Command("sha256sum", fmt.Sprintf("%s/%s/%s", cdnCachePath, dir, file)).CombinedOutput()
		fmt.Println(string(out))
		Expect(err).NotTo(HaveOccurred())
		sha256sum2 = strings.Split(string(out), " ")[0]
		fmt.Println(string(sha256sum2))
		break
	}
	return sha256sum2
}

// getCDNExec get cdn pods
func getCDNExec(n int) *e2eutil.PodExec {
	out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=cdn",
		"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", n)).CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "dragonfly-cdn-")).Should(BeTrue())
	return e2eutil.NewPodExec(dragonflyNamespace, podName, "cdn")
}

// getFileServerExec get the file-server pod for curl
func getFileServerExec() *e2eutil.PodExec {
	out, err := e2eutil.KubeCtlCommand("-n", e2eNamespace, "get", "pod", "-l", "component=file-server",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "file-server-")).Should(BeTrue())
	return e2eutil.NewPodExec(e2eNamespace, podName, "")
}
