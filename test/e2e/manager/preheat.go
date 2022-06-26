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
	"errors"
	"fmt"
	"strings"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/structure"
	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat files should be ok", func() {
			var seedPeerPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				seedPeerPods[i] = getSeedPeerExec(i)
			}
			fsPod := getFileServerExec()

			for _, v := range e2eutil.GetFileList() {
				url := e2eutil.GetFileURL(v)
				fmt.Println("download url: " + url)

				// get original file digest
				out, err := e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				fmt.Println("original sha256sum: " + string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum1 := strings.Split(string(out), " ")[0]

				// preheat file
				req, err := structure.StructToMap(types.CreatePreheatJobRequest{
					Type: internaljob.PreheatJob,
					Args: types.PreheatArgs{
						Type: "file",
						URL:  url,
					},
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
				seedPeerTaskID := idgen.TaskID(url, &base.UrlMeta{})
				fmt.Println(seedPeerTaskID)

				sha256sum, err := checkPreheatResult(seedPeerPods, seedPeerTaskID)
				Expect(err).NotTo(HaveOccurred())
				Expect(sha256sum1).To(Equal(sha256sum))
			}
		})

		It("preheat image should be ok", func() {
			url := "https://index.docker.io/v2/dragonflyoss/busybox/manifests/1.35.0"
			fmt.Println("download image: " + url)

			var (
				seedPeerTaskIDs = []string{
					"b6922209dc9616f8736a860e93c3cd7288a4e801517f88eec3df514606d18cdf",
					"c0dfae864ae65c285676063eb148d0a0064d5c6c39367fee0bcc1f3700c39c31",
				}
				sha256sum1 = []string{
					"a711f05d33845e2e9deffcfcc5adf082d7c6e97e3e3a881d193d9aae38f092a8",
					"f643e116a03d9604c344edb345d7592c48cc00f2a4848aaf773411f4fb30d2f5",
				}
			)

			var seedPeerPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				seedPeerPods[i] = getSeedPeerExec(i)
			}
			fsPod := getFileServerExec()

			// preheat file
			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "image",
					URL:  url,
				},
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

			for i, seedPeerTaskID := range seedPeerTaskIDs {
				sha256sum, err := checkPreheatResult(seedPeerPods, seedPeerTaskID)
				Expect(err).NotTo(HaveOccurred())
				Expect(sha256sum1[i]).To(Equal(sha256sum))
			}
		})
	})
})

func waitForDone(preheat *model.Job, pod *e2eutil.PodExec) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

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
			switch preheat.State {
			case machineryv1tasks.StateSuccess:
				return true
			case machineryv1tasks.StateFailure:
				return false
			default:
			}
		}
	}
}

func checkPreheatResult(seedPeerPods [3]*e2eutil.PodExec, seedPeerTaskID string) (string, error) {
	var sha256sum string
	for _, seedPeer := range seedPeerPods {
		taskDir := fmt.Sprintf("%s/%s", seedPeerDataPath, seedPeerTaskID)
		if _, err := seedPeer.Command("ls", taskDir).CombinedOutput(); err != nil {
			// if the directory does not exist, skip this seed peer
			fmt.Printf("directory %s does not exist: %s\n", taskDir, err.Error())
			continue
		}

		// calculate digest of downloaded file
		out, err := seedPeer.Command("sh", "-c", fmt.Sprintf("sha256sum %s/*/%s", taskDir, "data")).CombinedOutput()
		fmt.Println("preheat sha256sum: " + string(out))
		Expect(err).NotTo(HaveOccurred())
		sha256sum = strings.Split(string(out), " ")[0]
		break
	}

	if sha256sum == "" {
		return "", errors.New("can not found sha256sum")
	}

	return sha256sum, nil
}

// getSeedPeerExec get seed peer pods
func getSeedPeerExec(n int) *e2eutil.PodExec {
	out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=seed-peer",
		"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", n)).CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "dragonfly-seed-peer-")).Should(BeTrue())
	return e2eutil.NewPodExec(dragonflyNamespace, podName, "seed-peer")
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
