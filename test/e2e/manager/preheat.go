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

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/structure"
	"d7y.io/dragonfly/v2/test/e2e/util"
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat files should be ok", Label("preheat", "file"), func() {
			var seedPeerPods [3]*util.PodExec
			for i := 0; i < 3; i++ {
				seedPeerPods[i] = getSeedPeerExec(i)
			}
			fsPod := getFileServerExec()

			for _, v := range util.GetFileList() {
				url := util.GetFileURL(v)
				fmt.Println("download url: " + url)

				// get original file digest
				out, err := util.DockerCommand("sha256sum", v).CombinedOutput()
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
				job := &models.Job{}
				err = json.Unmarshal(out, job)
				Expect(err).NotTo(HaveOccurred())
				done := waitForDone(job, fsPod)
				Expect(done).Should(BeTrue())

				// generate task_id, also the filename
				seedPeerTaskID := idgen.TaskIDV1(url, &commonv1.UrlMeta{})
				fmt.Println(seedPeerTaskID)

				sha256sum, err := checkPreheatResult(seedPeerPods, seedPeerTaskID)
				Expect(err).NotTo(HaveOccurred())
				Expect(sha256sum1).To(Equal(sha256sum))
			}
		})

		It("preheat image should be ok", Label("preheat", "image"), func() {
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

			var seedPeerPods [3]*util.PodExec
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
			job := &models.Job{}
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

		It("preheat image for linux/amd64 platform should be ok", Label("preheat", "image"), func() {
			if !util.FeatureGates.Enabled(util.FeatureGatePreheatMultiArchImage) {
				fmt.Println("feature gate preheat multi arch image is disable, skip")
				return
			}

			url := "https://index.docker.io/v2/dragonflyoss/scheduler/manifests/v2.1.0"
			fmt.Println("download image: " + url)

			var (
				seedPeerTaskIDs = []string{
					"c8ca6a17354d3a79397eef26803e5af84d00a3fd64b0f823922086a31ebdee18",
					"b8de5865e2ebf537279683adfbdb5f858b0c7212e5744a1df233086496c245d7",
					"e4bf0d4b551afda56f9627c81ee02ab4360865d37c7dd43586e37f26f4386806",
					"7da0721fd078dd46a63298747ffde8fcbe12b53378f282c9def693615ac7993e",
					"3639c8c5712e77acd3751142c83150c0a12284a54fa41224a1c7acc0e343020d",
				}
				sha256sum1 = []string{
					"f1f1039835051ecc04909f939530e86a20f02d2ce5ad7a81c0fa3616f7303944",
					"c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
					"871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
					"f1a1d290795d904815786e41d39a41dc1af5de68a9e9020baba8bd83b32d8f95",
					"f1ffc4b5459e82dc8e7ddd1d1a2ec469e85a1f076090c22851a1f2ce6f71e1a6",
				}
			)

			var seedPeerPods [3]*util.PodExec
			for i := 0; i < 3; i++ {
				seedPeerPods[i] = getSeedPeerExec(i)
			}
			fsPod := getFileServerExec()

			// preheat file
			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type:     "image",
					URL:      url,
					Platform: "linux/amd64",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			job := &models.Job{}
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

		It("preheat image for linux/arm64 platform  should be ok", Label("preheat", "image"), func() {
			if !util.FeatureGates.Enabled(util.FeatureGatePreheatMultiArchImage) {
				fmt.Println("feature gate preheat multi arch image is disable, skip")
				return
			}

			url := "https://index.docker.io/v2/dragonflyoss/scheduler/manifests/v2.1.0"
			fmt.Println("download image: " + url)

			var (
				seedPeerTaskIDs = []string{
					"9869dbb01ac214e90e4ae667e42d50210c2ff1e63292d73b14f0a7a2226c0320",
					"ab049caee13f77d91568d954a5d32f5d2354497cab098887a8a663656daa9840",
					"e4bf0d4b551afda56f9627c81ee02ab4360865d37c7dd43586e37f26f4386806",
					"a26e1ac8b70926f45766fcf886f23a833793c39c62237bcda9ffeb158131c0d6",
					"7376f665077e91cd0dc410c00242ab88775e3eae19eca4b7b3a29ded14fc3754",
				}
				sha256sum1 = []string{
					"a0d7a8f11f7e25ca59f0bf470187dd9aa27e7ca951cf67a53c750deea5d3b076",
					"a880266d3b77f75696023df2da1ef66c3c565e0f70596242395c9e68de955c7c",
					"871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
					"9b5952218d7711195c6c6fbddbef2780507d20851ca68845d180397d1348f0d8",
					"889f4c960ac4ff70774e9c4cfa64efc4823ade0702d0f96c20ff0054ffbbe504",
				}
			)

			var seedPeerPods [3]*util.PodExec
			for i := 0; i < 3; i++ {
				seedPeerPods[i] = getSeedPeerExec(i)
			}
			fsPod := getFileServerExec()

			// preheat file
			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type:     "image",
					URL:      url,
					Platform: "linux/arm64",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			job := &models.Job{}
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

func waitForDone(preheat *models.Job, pod *util.PodExec) bool {
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

func checkPreheatResult(seedPeerPods [3]*util.PodExec, seedPeerTaskID string) (string, error) {
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
func getSeedPeerExec(n int) *util.PodExec {
	out, err := util.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=seed-peer",
		"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", n)).CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "dragonfly-seed-peer-")).Should(BeTrue())
	return util.NewPodExec(dragonflyNamespace, podName, "seed-peer")
}

// getFileServerExec get the file-server pod for curl
func getFileServerExec() *util.PodExec {
	out, err := util.KubeCtlCommand("-n", e2eNamespace, "get", "pod", "-l", "component=file-server",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "file-server-")).Should(BeTrue())
	return util.NewPodExec(e2eNamespace, podName, "")
}
