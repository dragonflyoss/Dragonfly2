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

var _ = Describe("Get and Delete with Manager", func() {
	Context("get and delete /bin/md5sum task", func() {
		It("get and delete task should be ok", func() {
			// Create preheat job
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
				ID:     "6ba5a8781902368d2b07eb8b6d6044a96f49d5008feace1ea8e3ebfc0b96d0a1",
				Sha256: "80f1d8cd843a98b23b30e90e7e43a14e05935351f354d678bc465f7be66ef3dd",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			// Check the file is downloaded successfully
			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			// Get task
			req, err = structure.StructToMap(types.CreateGetTaskJobRequest{
				Type: internaljob.GetTaskJob,
				Args: types.GetTaskArgs{
					TaskID: fileMetadata.ID,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			out, err = managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://127.0.0.1:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job = &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done = waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			// Check get task response is not null
			Expect(job.Result).NotTo(BeNil())

			// Delete task
			req, err = structure.StructToMap(types.CreateDeleteTaskJobRequest{
				Type: internaljob.DeleteTaskJob,
				Args: types.DeleteTaskArgs{
					TaskID: fileMetadata.ID,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			out, err = managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://127.0.0.1:8080/api/v1/jobs").CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			job = &models.Job{}
			err = json.Unmarshal(out, job)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			done = waitForDone(job, managerPod)
			Expect(done).Should(BeTrue())

			// Check delete task response is not null
			Expect(job.Result).NotTo(BeNil())

			// Check file is deleted successfully
			exist := util.CheckFilesExist(seedClientPods, fileMetadata.ID)
			Expect(exist).Should(BeFalse())
		})
	})
})
