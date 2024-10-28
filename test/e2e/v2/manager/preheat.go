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
	"encoding/json"
	"fmt"

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
				ID:     "b0a5cfd4ccf5310803675f742dedc435a64e9a5f539f48fedbef6c30aac18b7c",
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
				ID:     "802e3df5384438deaed066ca445489f6e314ebb6a2d4728d020e75a08d281942",
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
				ID:     "4f1de4716ec6d1ca56daf1f5dd2520a8f6a826d90474f596cdf99a5c88fef982",
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
					ID:     "b6922209dc9616f8736a860e93c3cd7288a4e801517f88eec3df514606d18cdf",
					Sha256: "a711f05d33845e2e9deffcfcc5adf082d7c6e97e3e3a881d193d9aae38f092a8",
				},
				{
					ID:     "c0dfae864ae65c285676063eb148d0a0064d5c6c39367fee0bcc1f3700c39c31",
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
					ID:     "c8ca6a17354d3a79397eef26803e5af84d00a3fd64b0f823922086a31ebdee18",
					Sha256: "f1f1039835051ecc04909f939530e86a20f02d2ce5ad7a81c0fa3616f7303944",
				},
				{
					ID:     "b8de5865e2ebf537279683adfbdb5f858b0c7212e5744a1df233086496c245d7",
					Sha256: "c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
				},
				{
					ID:     "e4bf0d4b551afda56f9627c81ee02ab4360865d37c7dd43586e37f26f4386806",
					Sha256: "871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
				},
				{
					ID:     "7da0721fd078dd46a63298747ffde8fcbe12b53378f282c9def693615ac7993e",
					Sha256: "f1a1d290795d904815786e41d39a41dc1af5de68a9e9020baba8bd83b32d8f95",
				},
				{
					ID:     "3639c8c5712e77acd3751142c83150c0a12284a54fa41224a1c7acc0e343020d",
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

		It("preheat image for linux/arm64 platform should be ok", Label("preheat", "image"), func() {
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
					ID:     "9869dbb01ac214e90e4ae667e42d50210c2ff1e63292d73b14f0a7a2226c0320",
					Sha256: "a0d7a8f11f7e25ca59f0bf470187dd9aa27e7ca951cf67a53c750deea5d3b076",
				},
				{
					ID:     "ab049caee13f77d91568d954a5d32f5d2354497cab098887a8a663656daa9840",
					Sha256: "a880266d3b77f75696023df2da1ef66c3c565e0f70596242395c9e68de955c7c",
				},
				{
					ID:     "e4bf0d4b551afda56f9627c81ee02ab4360865d37c7dd43586e37f26f4386806",
					Sha256: "871ab018db94b4ae7b137764837bc4504393a60656ba187189e985cd809064f7",
				},
				{
					ID:     "a26e1ac8b70926f45766fcf886f23a833793c39c62237bcda9ffeb158131c0d6",
					Sha256: "9b5952218d7711195c6c6fbddbef2780507d20851ca68845d180397d1348f0d8",
				},
				{
					ID:     "7376f665077e91cd0dc410c00242ab88775e3eae19eca4b7b3a29ded14fc3754",
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
