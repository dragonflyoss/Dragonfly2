/*
 *     Copyright 2024 The Dragonfly Authors
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
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/structure"
	"d7y.io/dragonfly/v2/test/e2e/v2/util"
)

var _ = Describe("Preheat with Manager", func() {
	Context("/bin/md5sum file", func() {
		It("preheat files should be ok", Label("preheat", "file"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/md5sum"),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://127.0.0.1:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			fileMetadata := util.FileMetadata{
				ID:     "e8ef42dcc1e8da5e77b19bf39532f91f0bfeb85ed0d3ce277e1823f91c5a255a",
				Sha256: "80f1d8cd843a98b23b30e90e7e43a14e05935351f354d678bc465f7be66ef3dd",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/toe file", func() {
		It("preheat files should be ok", Label("preheat", "file"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/toe"),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			fileMetadata := util.FileMetadata{
				ID:     "bf3cd4dd9b582ac4a9253d28d38f0d2cb942455572a2a2d2fc9a82e1e83eda4f",
				Sha256: "4c7f0f298ab3350859f90664d706b8ccaa95072f1f1f3dd74f559642e5483cd5",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/i386 file", func() {
		It("preheat files should be ok", Label("preheat", "file"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/i386"),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			fileMetadata := util.FileMetadata{
				ID:     "c6273a9e7140bc2af543fbb15e16186e8f07b054f7bfd1556dce5a76dba7dd28",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/jq file", func() {
		It("preheat files should be ok", Label("preheat", "file"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/jq"),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			fileMetadata := util.FileMetadata{
				ID:     "b9615c0754d6e5dafe0b3b8b1aafc836635efd528a3815288646728c946a0469",
				Sha256: "5a963cbdd08df27651e9c9d006567267ebb3c80f7b8fc0f218ade5771df2998b",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("busybox:v1.35.0 image", func() {
		It("preheat image should be ok", Label("preheat", "image"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "image",
					URL:  "https://index.docker.io/v2/dragonflyoss/busybox/manifests/1.35.0",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "22b576f99cff7676bdb10a4fdf1f1ae5163ccf9023d07d5f1be355a86c3e99e7",
					Sha256: "a711f05d33845e2e9deffcfcc5adf082d7c6e97e3e3a881d193d9aae38f092a8",
				},
				{
					ID:     "3ef8cc79ebac6ad32b68e0ea4b0a863b808b72377e645d1d87c73d073aea18d8",
					Sha256: "f643e116a03d9604c344edb345d7592c48cc00f2a4848aaf773411f4fb30d2f5",
				},
			}

			for _, taskMetadata := range taskMetadatas {
				seedClientPods := make([]*util.PodExec, 3)
				for i := 0; i < 3; i++ {
					seedClientPods[i], err = util.SeedClientExec(i)
					fmt.Println(err)
					Expect(err).NotTo(HaveOccurred())
				}

				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})
	})

	Context("dragonflyoss/scheduler:v2.1.0 image", func() {
		It("preheat image for linux/amd64 platform should be ok", Label("preheat", "image"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type:     "image",
					URL:      "https://index.docker.io/v2/dragonflyoss/scheduler/manifests/v2.1.0",
					Platform: "linux/amd64",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "db5beca8a19049e0420e4efa08983e6b22162c1192de39a363ed16ea6459ee28",
					Sha256: "f1f1039835051ecc04909f939530e86a20f02d2ce5ad7a81c0fa3616f7303944",
				},
				{
					ID:     "b89495f24d34cae8e9174df15b60d34d490547d3029e3c187079cc3b475ff250",
					Sha256: "c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
				},
				{
					ID:     "efc4e319971484d86cc43a48a9e3eccc23736cabc50ba4d1e707a841eaf42e12",
					Sha256: "871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
				},
				{
					ID:     "5ff373729cb097252966b0fad599bad4c87e0dc96bf77c65b91badc99d2f7e99",
					Sha256: "f1a1d290795d904815786e41d39a41dc1af5de68a9e9020baba8bd83b32d8f95",
				},
				{
					ID:     "99e12b50d80a25090c7928fe3ce35ca97bd373c45fe90870b3b70884bf9c34c9",
					Sha256: "f1ffc4b5459e82dc8e7ddd1d1a2ec469e85a1f076090c22851a1f2ce6f71e1a6",
				},
			}

			for _, taskMetadata := range taskMetadatas {
				seedClientPods := make([]*util.PodExec, 3)
				for i := 0; i < 3; i++ {
					seedClientPods[i], err = util.SeedClientExec(i)
					fmt.Println(err)
					Expect(err).NotTo(HaveOccurred())
				}

				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})

		It("preheat image for linux/arm64 platform  should be ok", Label("preheat", "image"), func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type:     "image",
					URL:      "https://index.docker.io/v2/dragonflyoss/scheduler/manifests/v2.1.0",
					Platform: "linux/arm64",
				},
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "4fed91258f022c3f8eb8c0408e871bff653bf8015f9e9ec71bd03a2bb639119b",
					Sha256: "a0d7a8f11f7e25ca59f0bf470187dd9aa27e7ca951cf67a53c750deea5d3b076",
				},
				{
					ID:     "fb47239bde6b518227ccce5ed86b0b1570a9e42ba94ca72f99f5db640350e22a",
					Sha256: "a880266d3b77f75696023df2da1ef66c3c565e0f70596242395c9e68de955c7c",
				},
				{
					ID:     "efc4e319971484d86cc43a48a9e3eccc23736cabc50ba4d1e707a841eaf42e12",
					Sha256: "871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
				},
				{
					ID:     "7860f1bd9cc5eca105df4c40719351562f04f5f0bda7805c34ed475ddd66d778",
					Sha256: "9b5952218d7711195c6c6fbddbef2780507d20851ca68845d180397d1348f0d8",
				},
				{
					ID:     "34be7b23dcbb09487133810e30c1e701a4285f742d4dbe6f6fda496014f90af6",
					Sha256: "889f4c960ac4ff70774e9c4cfa64efc4823ade0702d0f96c20ff0054ffbbe504",
				},
			}

			for _, taskMetadata := range taskMetadatas {
				seedClientPods := make([]*util.PodExec, 3)
				for i := 0; i < 3; i++ {
					seedClientPods[i], err = util.SeedClientExec(i)
					fmt.Println(err)
					Expect(err).NotTo(HaveOccurred())
				}

				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
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
				fmt.Sprintf("http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs/%d", preheat.ID)).CombinedOutput()
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
