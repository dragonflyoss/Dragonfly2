package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat should be ok", func() {
			// get cdn pods
			var cdnPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=cdn",
					"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", i)).CombinedOutput()
				podName := strings.Trim(string(out), "'")
				Expect(err).NotTo(HaveOccurred())
				fmt.Println(podName)
				Expect(strings.HasPrefix(podName, "dragonfly-cdn-")).Should(BeTrue())
				cdnPods[i] = e2eutil.NewPodExec(dragonflyNamespace, podName, "cdn")
			}

			// get file-server for curl
			out, err := e2eutil.KubeCtlCommand("-n", e2eNamespace, "get", "pod", "-l", "component=file-server",
				"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
			podName := strings.Trim(string(out), "'")
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(podName)
			Expect(strings.HasPrefix(podName, "file-server-")).Should(BeTrue())
			fsPod := e2eutil.NewPodExec(e2eNamespace, podName, "")

			for _, v := range e2eutil.GetFileList() {
				url := e2eutil.GetFileURL(v)
				fmt.Println("download url " + url)

				// get original file digest
				out, err = e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum1 := strings.Split(string(out), " ")[0]

				// preheat file
				out, err = fsPod.CurlCommand("POST", "Content-Type:application/json",
					fmt.Sprintf(`{"type":"file","url":"%s"}`, url),
					fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())

				// wait for success
				preheatJob := &types.Preheat{}
				err = json.Unmarshal(out, preheatJob)
				Expect(err).NotTo(HaveOccurred())
				done := waitForDone(preheatJob, fsPod)
				Expect(done).Should(BeTrue())

				// generate task_id, also the filename
				taskID := idgen.TaskID(url, &base.UrlMeta{Tag: "d7y/manager"})
				fmt.Println(taskID)

				sha256sum2 := checkPreheatResult(cdnPods, taskID)
				if sha256sum2 == "" {
					fmt.Println("preheat file not found")
				}
				Expect(sha256sum1).To(Equal(sha256sum2))
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

			// get cdn pods
			var cdnPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				out, err = e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=cdn",
					"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", i)).CombinedOutput()
				podName := strings.Trim(string(out), "'")
				Expect(err).NotTo(HaveOccurred())
				fmt.Println(podName)
				Expect(strings.HasPrefix(podName, "dragonfly-cdn-")).Should(BeTrue())
				cdnPods[i] = e2eutil.NewPodExec(dragonflyNamespace, podName, "cdn")
			}

			// get file-server for curl
			out, err = e2eutil.KubeCtlCommand("-n", e2eNamespace, "get", "pod", "-l", "component=file-server",
				"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
			podName := strings.Trim(string(out), "'")
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(podName)
			Expect(strings.HasPrefix(podName, "file-server-")).Should(BeTrue())
			fsPod := e2eutil.NewPodExec(e2eNamespace, podName, "")

			// use a curl to preheat the same file, git a id to wait for success
			out, err = fsPod.CurlCommand("POST", "Content-Type:application/json",
				fmt.Sprintf(`{"type":"file","url":"%s"}`, url),
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			preheatJob := &types.Preheat{}
			err = json.Unmarshal(out, preheatJob)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(preheatJob, fsPod)
			Expect(done).Should(BeTrue())

			// generate task id to find the file
			taskID := idgen.TaskID(url, &base.UrlMeta{Tag: "d7y/manager"})
			fmt.Println(taskID)

			sha256sum2 := checkPreheatResult(cdnPods, taskID)
			if sha256sum2 == "" {
				fmt.Println("preheat file not found")
			}
			Expect(sha256sum1).To(Equal(sha256sum2))
		})
	})
})

func waitForDone(preheat *types.Preheat, pod *e2eutil.PodExec) bool {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			out, err := pod.CurlCommand("", "", "",
				fmt.Sprintf("http://%s:%s/%s/%s", managerService, managerPort, preheatPath, preheat.ID)).CombinedOutput()
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

func checkPreheatResult(cdnPods [3]*e2eutil.PodExec, taskID string) string {
	var sha256sum2 string
	for _, cdn := range cdnPods {
		out, err := cdn.Command("ls", cdnCachePath).CombinedOutput()
		if err != nil {
			// if the directory does not exist, skip this cdn
			continue
		}
		// directory name is the first three characters of the task id
		dir := taskID[0:3]
		if !strings.Contains(string(out), dir) {
			continue
		}

		out, err = cdn.Command("ls", fmt.Sprintf("%s/%s", cdnCachePath, dir)).CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		// file name is the same as task id
		file := taskID
		if !strings.Contains(string(out), file) {
			continue
		}

		// calculate digest of downloaded file
		out, err = cdn.Command("sha256sum", fmt.Sprintf("%s/%s/%s", cdnCachePath, dir, file)).CombinedOutput()
		fmt.Println(string(out))
		Expect(err).NotTo(HaveOccurred())
		sha256sum2 = strings.Split(string(out), " ")[0]
		break
	}
	return sha256sum2
}
