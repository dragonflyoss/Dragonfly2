package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"

	"d7y.io/dragonfly/v2/manager/types"

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

const (
	cdnCachePath = "/tmp/cdn/download"
	preheatUrl   = "http://dragonfly-manager.dragonfly-system.svc:8080/api/v1/preheats"
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat should be ok", func() {
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

			for _, v := range e2eutil.GetFileList() {
				//for _, cdn := range cdnPods {
				//	cdn.Command("rm", "-rf", cdnCachePath).CombinedOutput()
				//	cdn.Command("mkdir", cdnCachePath).CombinedOutput()
				//}

				url := e2eutil.GetFileURL(v)
				fmt.Println("download url " + url)

				// get original file digest
				//out, err := e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				//fmt.Println(string(out))
				//Expect(err).NotTo(HaveOccurred())
				//sha256sum1 := strings.Split(string(out), " ")[0]

				// preheat file
				out, err := cdnPods[0].Command("curl", "-s", "-X", "POST", "-H", "Content-Type:application/json",
					"-d", fmt.Sprintf(`{"type":"file","url":"%s"}`, url), preheatUrl).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())

				// wait for success
				preheatJob := &types.Preheat{}
				err = json.Unmarshal(out, preheatJob)
				Expect(err).NotTo(HaveOccurred())
				waitForDone(preheatJob, cdnPods[0])

				// get downloaded file digest
				//var sha256sum2 string
				//for _, cdn := range cdnPods {
				//	out, err = cdn.Command("ls", cdnCachePath).CombinedOutput()
				//	fmt.Println(string(out))
				//	dir := strings.Split(string(out), "\n")[0]
				//	if len(dir) != 3 {
				//		continue
				//	}
				//	fmt.Println(dir)
				//
				//	out, err = cdn.Command("ls", fmt.Sprintf("%s/%s/", cdnCachePath, dir)).CombinedOutput()
				//	Expect(err).NotTo(HaveOccurred())
				//	file := strings.Split(strings.Split(string(out), "\n")[0], ".")[0]
				//	if len(file) != 64 {
				//		continue
				//	}
				//	fmt.Println(file)
				//
				//	out, err = cdn.Command("sha256sum", fmt.Sprintf("%s/%s/%s", cdnCachePath, dir, file)).CombinedOutput()
				//	fmt.Println(string(out))
				//	Expect(err).NotTo(HaveOccurred())
				//	sha256sum2 = strings.Split(string(out), " ")[0]
				//	break
				//}
				//Expect(sha256sum1).To(Equal(sha256sum2))
			}
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
			out, err := pod.Command("curl", "-s", fmt.Sprintf("%s/%s", preheatUrl, preheat.ID)).CombinedOutput()
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
