package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
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
		It("should handle stopping some scheduler instances during preheat", Label("preheat", "scheduler"), func() {
			// Start preheat job.
			managerPod, err := util.ManagerExec(0)
			Expect(err).NotTo(HaveOccurred(), "Failed to execute manager pod")

			req, err := structure.StructToMap(types.CreatePreheatJobRequest{
				Type: internaljob.PreheatJob,
				Args: types.PreheatArgs{
					Type: "file",
					URL:  util.GetFileURL("/bin/md5sum"),
				},
			})
			Expect(err).NotTo(HaveOccurred(), "Failed to convert request to map")

			out, err := managerPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"}, req,
				"http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/jobs").CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), "Failed to start preheat job")
			fmt.Println(string(out))

			job := &models.Job{}
			err = json.Unmarshal(out, job)
			Expect(err).NotTo(HaveOccurred(), "Failed to unmarshal job response")
			fmt.Printf("Job ID: %d\n", job.ID)

			// Get scheduler pods
			schedulerPods, err := getSchedulerPods()
			Expect(err).NotTo(HaveOccurred(), "Failed to get scheduler pods")

			if len(schedulerPods) < 2 {
				Fail("Not enough scheduler pods found")
			}

			// Randomly select a few scheduler pods to delete
			numToStop := 2
			rand.Shuffle(len(schedulerPods), func(i, j int) {
				schedulerPods[i], schedulerPods[j] = schedulerPods[j], schedulerPods[i]
			})

			for i := 0; i < numToStop; i++ {
				podName := schedulerPods[i]
				fmt.Printf("Stopping scheduler pod %s\n", podName)
				err := util.KubeCtlCommand("-n", "dragonfly-system", "delete", "pod", podName).Run()
				Expect(err).NotTo(HaveOccurred(), "Failed to delete scheduler pod")
			}

			// Wait for the job to complete.
			done := waitForDone(job, managerPod)
			Expect(done).Should(BeTrue(), "Preheat job did not complete successfully")

			// Check file integrity.
			fileMetadata := util.FileMetadata{
				ID:     "6ba5a8781902368d2b07eb8b6d6044a96f49d5008feace1ea8e3ebfc0b96d0a1",
				Sha256: "80f1d8cd843a98b23b30e90e7e43a14e05935351f354d678bc465f7be66ef3dd",
			}

			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				Expect(err).NotTo(HaveOccurred(), "Failed to execute seed client pod")
			}

			sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred(), "Failed to calculate SHA256")
			Expect(fileMetadata.Sha256).To(Equal(sha256sum), "SHA256 mismatch")
		})
	})
})

func getSchedulerPods() ([]string, error) {
	out, err := util.KubeCtlCommand("-n", "dragonfly-system", "get", "pod", "-l", "component=scheduler",
		"-o", "jsonpath='{.items[*].metadata.name}'").CombinedOutput()
	if err != nil {
		return nil, err
	}

	pods := strings.Fields(string(out))
	return pods, nil
}

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
			Expect(err).NotTo(HaveOccurred(), "Failed to get job status")
			err = json.Unmarshal(out, preheat)
			Expect(err).NotTo(HaveOccurred(), "Failed to unmarshal job status")
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
