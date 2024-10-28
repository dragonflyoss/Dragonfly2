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

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	"d7y.io/dragonfly/v2/test/e2e/v2/util"
)

var _ = Describe("Download Using Dfget", func() {
	Context("/etc/containerd/config.toml file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml-1"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "f82afbc76aa4d6472a982a8b8383a98d22c18c9ab06d24e989d9d4d30a49dfee",
				Sha256: "6288d2a89e2a9611191c25a45de20e94d8d058c75f274a39970d41f60f367e6f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-1"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "144eb7b6f67b8423c9999b9b864a35562aeff3e7d153eed2bb7f917977796bdf",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/x86_64 file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/x86_64"), util.GetOutputPath("x86_64-1"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "7327d3aa45f16e60396d687d8c81a9d5ec9c6ca662ad9c6dad77f830c499d5e3",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("x86_64-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/zless file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/zless"), util.GetOutputPath("zless-1"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "92709a5ce8a45db2563081b843403eec5d72f8a78fe2be85ce23e0edf8b2d207",
				Sha256: "b0cfe211f851049a78f5812cf5b1d7cb4f3fbb101c02c1865c940d5797f4b73b",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("zless-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/bash file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/bash"), util.GetOutputPath("bash-1"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "56ac4873dda3db55ed78c6b26b24d51343803882714a54802bfddbb1097eb06d",
				Sha256: "c37f93c73cf2f303f874c094f6f76e47b2421a3da9f0e7e0b98bea8a3d685322",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("bash-1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/etc/containerd/config.toml file and set application to d7y", func() {
		It("download should be ok", Label("dfget", "download", "application d7y"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --application d7y --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml-3"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "fbf82ee8c8fb2f234c46947e95b4b51fc9a01b9306e52c6b717f5323b6e7c5e4",
				Sha256: "6288d2a89e2a9611191c25a45de20e94d8d058c75f274a39970d41f60f367e6f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml-3"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/etc/containerd/config.toml file and set tag to d7y", func() {
		It("download should be ok", Label("dfget", "download", "tag d7y"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --tag d7y --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml-4"))).CombinedOutput()
			fmt.Println(string(out), err)
			Expect(err).NotTo(HaveOccurred())

			fileMetadata := util.FileMetadata{
				ID:     "fbf82ee8c8fb2f234c46947e95b4b51fc9a01b9306e52c6b717f5323b6e7c5e4",
				Sha256: "6288d2a89e2a9611191c25a45de20e94d8d058c75f274a39970d41f60f367e6f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml-4"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}

			sha256sum, err = util.CalculateSha256ByTaskID(seedClientPods, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})
})
