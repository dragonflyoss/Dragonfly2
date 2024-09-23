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

var _ = Describe("GetTask and DeleteTask with Manager", func() {
	Context("/bin/cat file", Label("getTask", "deleteTask", "file"), func() {
		It("getTask and deleteTask should be ok", func() {
			// Create preheat job.
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/cat"),
				},
				SchedulerClusterIDs: []uint{1},
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
				ID:     "6f1e003b51a34df01dd80e3498dbaad584584d97888464b33b3e2c8442a3d485",
				Sha256: "df954abca766aceddd79dd20429e4f222019018667446626d3a641d3c47c50fc",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			// Check the file is downloaded successfully.
			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			// Get task.
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

			// Check get task response is valid.
			isValid := checkTaskResponse(job)
			Expect(isValid).To(BeTrue())

			// Delete task.
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

			// Check delete task response is valid.
			isValid = checkTaskResponse(job)
			Expect(isValid).To(BeTrue())

			// Check file is deleted successfully.
			exist := util.CheckFilesExist(seedClientPods, fileMetadata.ID)
			Expect(exist).Should(BeFalse())
		})
	})

	Context("/bin/pwd file", Label("getTask", "deleteTask", "file"), func() {
		It("getTask and deleteTask should be ok", func() {
			// Create preheat job.
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/pwd"),
				},
				SchedulerClusterIDs: []uint{1},
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
				ID:     "ddc397119488e07671cdeec0bf54c31b544ce2fbff14441c7444f1dddda2bd91",
				Sha256: "5286873505a9671e077f346cdfb89d5a6c99985fe3f11a972f30fedf9029bae0",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			// Check the file is downloaded successfully.
			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			// Get task.
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

			// Check get task response is valid.
			isValid := checkTaskResponse(job)
			Expect(isValid).To(BeTrue())

			// Delete task.
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

			// Check delete task response is valid.
			isValid = checkTaskResponse(job)
			Expect(isValid).To(BeTrue())

			// Check file is deleted successfully.
			exist := util.CheckFilesExist(seedClientPods, fileMetadata.ID)
			Expect(exist).Should(BeFalse())
		})
	})

	Context("/bin/time file", Label("getTask", "deleteTask", "file"), func() {
		It("getTask and deleteTask should be failed", func() {
			managerPod, err := util.ManagerExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "d7704dffb62c1925c56c2ad4e04ba16daf266432eed43a59336e8bd6e71fab92",
				Sha256: "7301b9b4c51a8f4d26c1af0da250f03a49ec8a8141033123e79196ad18f6c81b",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			// Check the file is not exist.
			exist := util.CheckFilesExist(seedClientPods, fileMetadata.ID)
			Expect(exist).Should(BeFalse())

			// Get task.
			req, err := structure.StructToMap(types.CreateGetTaskJobRequest{
				Type: internaljob.GetTaskJob,
				Args: types.GetTaskArgs{
					TaskID: fileMetadata.ID,
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

			// Check get task response is invalid.
			isValid := checkTaskResponse(job)
			Expect(isValid).To(BeFalse())

			// Delete task.
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

			// Check delete task response is invalid.
			isValid = checkTaskResponse(job)
			Expect(isValid).To(BeFalse())
		})
	})
})

func checkTaskResponse(job *models.Job) bool {
	// Check task response is not null.
	Expect(job.Result).NotTo(BeNil())
	groupJobStateData, err := json.Marshal(job.Result)
	Expect(err).NotTo(HaveOccurred())
	groupJobState := internaljob.GroupJobState{}
	err = json.Unmarshal(groupJobStateData, &groupJobState)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(groupJobState.JobStates)).Should(BeNumerically("==", 3))

	// Check task response is valid.
	for _, state := range groupJobState.JobStates {
		for _, result := range state.Results {
			resultData, err := json.Marshal(result)
			Expect(err).NotTo(HaveOccurred())

			switch job.Type {
			case internaljob.GetTaskJob:
				getTaskResponse := internaljob.GetTaskResponse{}
				err = json.Unmarshal(resultData, &getTaskResponse)
				Expect(err).NotTo(HaveOccurred())
				if len(getTaskResponse.Peers) > 0 {
					return true
				}
			case internaljob.DeleteTaskJob:
				deleteTaskResponse := internaljob.DeleteTaskResponse{}
				err = json.Unmarshal(resultData, &deleteTaskResponse)
				Expect(err).NotTo(HaveOccurred())
				if len(deleteTaskResponse.SuccessTasks) > 0 || len(deleteTaskResponse.FailureTasks) > 0 {
					return true
				}
			}
		}
	}

	return false
}
