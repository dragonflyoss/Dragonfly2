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

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	"d7y.io/dragonfly/v2/test/e2e/v2/util"
)

var _ = Describe("Download with dfget", func() {
	Context("/etc/containerd/config.toml file", func() {
		It("download should be ok", Label("dfget", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "1fc5ed9922a3d741063c169ec49c2071a391db5fda8de30eb6a97f60b5038c16",
				Sha256: "66404431f9a0d5c78205e5a3eb041f76767094fc278c0a091d4ffa10f06cf641",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "aaa32162d94ffb18dd407dc9abf2ac915b6dac4687dcf936a364818717d0155b",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/x86_64"), util.GetOutputPath("x86_64"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "3e9135a3e652efb9e6ae0b430f61d36a91093208009ddb839a1c9a1979274f89",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("x86_64"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/zless"), util.GetOutputPath("zless"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "a36732ab9c09237884ceefe8c1a007558fd15a9c891f1f905e4b95136266da70",
				Sha256: "b0cfe211f851049a78f5812cf5b1d7cb4f3fbb101c02c1865c940d5797f4b73b",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("zless"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --output %s", util.GetFileURL("/bin/bash"), util.GetOutputPath("bash"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "f2efea3df39f19e192a395843f67cfbb4338f3616014d9c5857da4c14cd01621",
				Sha256: "c37f93c73cf2f303f874c094f6f76e47b2421a3da9f0e7e0b98bea8a3d685322",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("bash"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

	Context("/etc/containerd/config.toml file and set piece length to 128MB", func() {
		It("download should be ok", Label("dfget", "download", "piece length 128MB"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --piece-length 134217728 --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "1ae51fe69c381a4604517f1d00d4315afef070bab3bcb475f11770fc5b194821",
				Sha256: "66404431f9a0d5c78205e5a3eb041f76767094fc278c0a091d4ffa10f06cf641",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

	Context("/bin/kubectl file and set piece length to 64MB", func() {
		It("download should be ok", Label("dfget", "download", "piece length 64MB"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --piece-length 67108864 --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "617d59d9cf3f9bd394ee98f327a77fc0b45a34431e59938abd0db20b467d8713",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

	Context("/bin/x86_64 file and set piece length to 32MB", func() {
		It("download should be ok", Label("dfget", "download", "piece length 32MB"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --piece-length 33554432 --output %s", util.GetFileURL("/bin/x86_64"), util.GetOutputPath("x86_64"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "f6b341fff271d4508e6f4820511c44bac3027c005c237798f98b89743c311148",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("x86_64"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

	Context("/bin/zless file and set piece length to 16MB", func() {
		It("download should be ok", Label("dfget", "download", "piece length 16MB"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --piece-length 16777216 --output %s", util.GetFileURL("/bin/zless"), util.GetOutputPath("zless"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "c358a010a628bd2720634ca245886c06d6555db7c020561bbea96d6c3afc13c9",
				Sha256: "b0cfe211f851049a78f5812cf5b1d7cb4f3fbb101c02c1865c940d5797f4b73b",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("zless"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

	Context("/bin/bash file and set piece length to 1MB", func() {
		It("download should be ok", Label("dfget", "download", "piece length 1MB"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --piece-length 1048576 --output %s", util.GetFileURL("/bin/bash"), util.GetOutputPath("bash"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "08b79b117296dd83c010d566c11b77e457d8021feb858f2d7b351686f12204e7",
				Sha256: "c37f93c73cf2f303f874c094f6f76e47b2421a3da9f0e7e0b98bea8a3d685322",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("bash"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --application d7y --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "9746acdb4bd8bf2deeb5dd8a3275e51a7fdd4adf8b0dc1d9d26a4565d3ed6592",
				Sha256: "66404431f9a0d5c78205e5a3eb041f76767094fc278c0a091d4ffa10f06cf641",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("dfget %s --disable-back-to-source --tag d7y --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config.toml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(out)

			fileMetadata := util.FileMetadata{
				ID:     "9746acdb4bd8bf2deeb5dd8a3275e51a7fdd4adf8b0dc1d9d26a4565d3ed6592",
				Sha256: "66404431f9a0d5c78205e5a3eb041f76767094fc278c0a091d4ffa10f06cf641",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config.toml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

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
