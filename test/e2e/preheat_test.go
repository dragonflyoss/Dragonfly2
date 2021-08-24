package e2e

import (
	"fmt"
	"strings"

	"d7y.io/dragonfly/v2/test/e2e/e2eutil"
	. "github.com/onsi/ginkgo" //nolint
	. "github.com/onsi/gomega" //nolint
)

var _ = Describe("Preheat with manager", func() {
	Context("preheat", func() {
		It("preheat should be ok", func() {
			out, err := e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pods").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

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

			var managerPods [3]*e2eutil.PodExec
			for i := 0; i < 3; i++ {
				out, err = e2eutil.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "component=manager",
					"-o", fmt.Sprintf("jsonpath='{range .items[%d]}{.metadata.name}{end}'", i)).CombinedOutput()
				podName := strings.Trim(string(out), "'")
				Expect(err).NotTo(HaveOccurred())
				fmt.Println(podName)
				Expect(strings.HasPrefix(podName, "dragonfly-manager-")).Should(BeTrue())
				managerPods[i] = e2eutil.NewPodExec(dragonflyNamespace, podName, "manager")
			}

			for _, v := range e2eutil.GetFileList() {
				url := e2eutil.GetFileURL(v)
				fmt.Println("download url " + url)

				// get original file digest
				out, err = e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				//sha256sum1 := strings.Split(string(out), " ")[0]

				// download file
				out, err = managerPods[0].Command("curl", "-X", "POST", "-H", `'Content-Type:application/json'`,
					"-d", fmt.Sprintf(`'{"type": "file", "url": "%s"}'`, url), "http://dragonfly-manager/preheats").CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())

				// get downloaded file digest
				//out, err = pod.Command("sha256sum", "/tmp/d7y.out").CombinedOutput()
				//fmt.Println(string(out))
				//Expect(err).NotTo(HaveOccurred())
				//sha256sum2 := strings.Split(string(out), " ")[0]
				//
				//Expect(sha256sum1).To(Equal(sha256sum2))
			}
		})
	})
})
