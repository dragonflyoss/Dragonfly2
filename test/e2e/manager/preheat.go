package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"

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
		It("preheat files should be ok", func() {
			cdnPods := getCDNs()
			fsPod := getFS()

			for _, v := range e2eutil.GetFileList() {
				url := e2eutil.GetFileURL(v)
				fmt.Println("download url " + url)

				// get original file digest
				out, err := e2eutil.DockerCommand("sha256sum", v).CombinedOutput()
				fmt.Println(string(out))
				Expect(err).NotTo(HaveOccurred())
				sha256sum1 := strings.Split(string(out), " ")[0]

				// preheat file
				out, err = fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"},
					map[string]interface{}{"type": "file", "url": url},
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
				taskID := idgen.TaskID(url, &base.UrlMeta{Tag: managerTag})
				fmt.Println(taskID)

				sha256sum2 := checkPreheatResult(cdnPods, taskID)
				if sha256sum2 == "" {
					fmt.Println("preheat file not found")
				}
				Expect(sha256sum1).To(Equal(sha256sum2))
			}
		})

		It("preheat image should be ok", func() {
			url := "https://registry-1.docker.io/v2/library/alpine/manifests/3.14"
			fmt.Println("download image " + url)

			taskIDs, digests, err := getLayers(url)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(taskIDs)

			cdnPods := getCDNs()
			fsPod := getFS()

			// preheat file
			out, err := fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"},
				map[string]interface{}{"type": "image", "url": url},
				fmt.Sprintf("http://%s:%s/%s", managerService, managerPort, preheatPath)).CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// wait for success
			preheatJob := &types.Preheat{}
			err = json.Unmarshal(out, preheatJob)
			Expect(err).NotTo(HaveOccurred())
			done := waitForDone(preheatJob, fsPod)
			Expect(done).Should(BeTrue())

			for i, taskID := range taskIDs {
				digest2 := checkPreheatResult(cdnPods, taskID)
				if digest2 == "" {
					fmt.Println("preheat file not found")
				}
				Expect(digests[i]).To(Equal(digest2))
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

			cdnPods := getCDNs()
			fsPod := getFS()

			// use a curl to preheat the same file, git a id to wait for success
			out, err = fsPod.CurlCommand("POST", map[string]string{"Content-Type": "application/json"},
				map[string]interface{}{"type": "file", "url": url},
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
			taskID := idgen.TaskID(url, &base.UrlMeta{Tag: managerTag})
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
			out, err := pod.CurlCommand("", nil, nil,
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

// getCDNs get cdn pods
func getCDNs() [3]*e2eutil.PodExec {
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
	return cdnPods
}

// getFS get the file-server pod for curl
func getFS() *e2eutil.PodExec {
	out, err := e2eutil.KubeCtlCommand("-n", e2eNamespace, "get", "pod", "-l", "component=file-server",
		"-o", "jsonpath='{range .items[*]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "file-server-")).Should(BeTrue())
	return e2eutil.NewPodExec(e2eNamespace, podName, "")
}

func getLayers(url string) (taskIDs, digests []string, err error) {
	header := http.Header{}
	resp, err := getManifests(url, header)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusUnauthorized {
			token := getAuthToken(resp.Header)
			bearer := "Bearer " + token
			header.Add("Authorization", bearer)

			resp, err = getManifests(url, header)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("request registry %d", resp.StatusCode)
		}
	}

	return parseLayers(resp)
}

func getManifests(url string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = header
	req.Header.Add("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func getAuthToken(header http.Header) (token string) {
	authURL := authURL(header.Values("WWW-Authenticate"))
	if len(authURL) == 0 {
		return
	}

	resp, err := http.Get(authURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	if result["token"] != nil {
		token = fmt.Sprintf("%v", result["token"])
	}
	return
}

func authURL(wwwAuth []string) string {
	// Bearer realm="<auth-service-url>",service="<service>",scope="repository:<name>:pull"
	if len(wwwAuth) == 0 {
		return ""
	}
	polished := make([]string, 0)
	for _, it := range wwwAuth {
		polished = append(polished, strings.ReplaceAll(it, "\"", ""))
	}
	fileds := strings.Split(polished[0], ",")
	host := strings.Split(fileds[0], "=")[1]
	query := strings.Join(fileds[1:], "&")
	return fmt.Sprintf("%s?%s", host, query)
}

func parseLayers(resp *http.Response) (taskIDs, digests []string, err error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, nil, err
	}

	for _, v := range manifest.References() {
		digest := v.Digest.String()
		if !strings.Contains(digest, ":") {
			return nil, nil, fmt.Errorf("get wrong digest")
		}
		digests = append(digests, strings.Split(digest, ":")[1])
		taskIDs = append(taskIDs, idgen.TaskID(fmt.Sprintf("https://registry-1.docker.io/v2/library/alpine/blobs/%s", digest),
			&base.UrlMeta{Digest: digest, Tag: managerTag}))
	}

	return taskIDs, digests, err
}
