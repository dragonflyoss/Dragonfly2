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

var _ = Describe("Download Using Proxy", func() {
	Context("/etc/containerd/config.toml file", func() {
		It("download should be ok", Label("proxy", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: proxy' %s --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config-proxy.coml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "090278fc27871e8bb93c4546a0f44dd16126ed476a23bee7c4b0812357e2042b",
				Sha256: "6288d2a89e2a9611191c25a45de20e94d8d058c75f274a39970d41f60f367e6f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("config-proxy.coml"))
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
		It("download should be ok", Label("proxy", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: proxy' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "80c17d9f21dcfd8b6f4526d131e65cd70dd0ad644033ea1900ea9a7deeed6dc3",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy"))
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
		It("download should be ok", Label("proxy", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: proxy' %s --output %s", util.GetFileURL("/bin/x86_64"), util.GetOutputPath("x86_64-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "559fb82754b3cd804f75d4e9e58ae6cd9aba5c49e24e7f52891bee5c815ed2b8",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("x86_64-proxy"))
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
		It("download should be ok", Label("proxy", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: proxy' %s --output %s", util.GetFileURL("/bin/zless"), util.GetOutputPath("zless-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "c8e05b7c9ebae46268691292fd2905c2cb4250af12ce91e53750e16eec69ce5a",
				Sha256: "b0cfe211f851049a78f5812cf5b1d7cb4f3fbb101c02c1865c940d5797f4b73b",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("zless-proxy"))
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
		It("download should be ok", Label("proxy", "download"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: proxy' %s --output %s", util.GetFileURL("/bin/bash"), util.GetOutputPath("bash-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "8a541eb07998f3980807ec1d183d74516bf6e2afe76d674e813ddca99938e02e",
				Sha256: "c37f93c73cf2f303f874c094f6f76e47b2421a3da9f0e7e0b98bea8a3d685322",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("bash-proxy"))
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

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=-100"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r -100 -H 'X-Dragonfly-Tag: proxy-bytes-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "ef524d1361ee2c0f0b0a457f1d5e605b3fe4f3dd2fd131950004bb20fa83d6fa",
				Sha256: "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("81d9f55e659a1b764e99dec6915ed47aa66955d30c47ffd4d427b4194a31684d").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-100"))
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
			Expect("81d9f55e659a1b764e99dec6915ed47aa66955d30c47ffd4d427b4194a31684d").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=0-100"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-100 -H 'X-Dragonfly-Tag: proxy-bytes-0-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-0-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "d22bb2076dc27f0f0be5d3ecc1e95a63ea516ce5dff49d235cf2266e98625474",
				Sha256: "7bbfcb694f6acc69483751d3b48d5bdbdcb284f9e04c7b7caa04b5977cffddc8",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-0-100"))
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
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100- -H 'X-Dragonfly-Tag: proxy-bytes-100-' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "f58775d042d37c406dbe03a294446e994f183e5872e93477025ad049f62a0a85",
				Sha256: "ba9a10ceceb80562fc124dc9bc94ea2a38e3a71e3e746e2140e6381ac791cdeb",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-100-"))
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
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-46026751"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-46026751 -H 'X-Dragonfly-Tag: proxy-bytes-100-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "6353c7c65066bf24acdfe62601ba17a1636d217169e79f7ea3f2ebf7f0940e06",
				Sha256: "ba9a10ceceb80562fc124dc9bc94ea2a38e3a71e3e746e2140e6381ac791cdeb",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-100-46026751"))
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
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=0-46026751"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-46026751 -H 'X-Dragonfly-Tag: proxy-bytes-0-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-0-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "1ff704a22e421b2f9b99f7f8e2228e6f71d9c987ef242348acc031d003d3e941",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-0-46026751"))
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
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-10240"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-10240 -H 'X-Dragonfly-Tag: proxy-bytes-100-10240' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-10240"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "dba445f5310c2a63fabeaad075511357e411b74f68a47bc579ed36f78a380013",
				Sha256: "8732360b941ad09a5e0e5d5f9891118bc068f6d0d5a56e3c6d483e4600fbc43f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-100-10240"))
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
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-1024"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-1024 -H 'X-Dragonfly-Tag: proxy-bytes-100-1024' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-1024"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "ae82ec86b58e7a91853ce6a3d52a59a1d590365a91b30db57df922bd8a6bc952",
				Sha256: "d6d17dca18b8de59e38da525dc24c47b74fec1a790a9f64afdd6538f4f8fa90e",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{clientPod}, util.GetOutputPath("kubectl-proxy-bytes-100-1024"))
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
			Expect("0dd6bcd036002bbd79cd8dfebcd9813ec4231562c0b15b02cbc7c70a062717f9").To(Equal(sha256sum))
		})
	})
})

var _ = Describe("Download Using Prefetch Proxy", func() {
	Context("/etc/containerd/config.toml file", func() {
		It("download should be ok", Label("prefetch-proxy", "download"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: prefetch-proxy' %s --output %s", util.GetFileURL("/etc/containerd/config.toml"), util.GetOutputPath("config-prefetch-proxy.coml"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "4a4df08f3aa3311e29556582de4269294a6805f81b7d8d38b83e8a91bc461d4e",
				Sha256: "6288d2a89e2a9611191c25a45de20e94d8d058c75f274a39970d41f60f367e6f",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("config-prefetch-proxy.coml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file", func() {
		It("download should be ok", Label("prefetch-proxy", "download"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: prefetch-proxy' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "3496028e92e0e35651f92e86ffde1dfa3c03a66265d248f8d3254ac78d30ef83",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/x86_64 file", func() {
		It("download should be ok", Label("prefetch-proxy", "download"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: prefetch-proxy' %s --output %s", util.GetFileURL("/bin/x86_64"), util.GetOutputPath("x86_64-prefetch-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "91ae78d5302e267bd245bae82c6ab90e18a8a4234bb87642f108901e801a1e2a",
				Sha256: "a1cbf1bf2d66757121677fd7fefafacd4f843a2cb44a451131002803bae56a65",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("x86_64-prefetch-proxy"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/zless file", func() {
		It("download should be ok", Label("prefetch-proxy", "download"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: prefetch-proxy' %s --output %s", util.GetFileURL("/bin/zless"), util.GetOutputPath("zless-prefetch-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "d2ccf9401074d729a88359394f1d9b4888ee6b7d9e4045e49bf1fa8d7caaf1d7",
				Sha256: "b0cfe211f851049a78f5812cf5b1d7cb4f3fbb101c02c1865c940d5797f4b73b",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("zless-prefetch-proxy"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/bash file", func() {
		It("download should be ok", Label("prefetch-proxy", "download"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -H 'X-Dragonfly-Tag: prefetch-proxy' %s --output %s", util.GetFileURL("/bin/bash"), util.GetOutputPath("bash-prefetch-proxy"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "a8a7a947d233ce5298c937c7e3cb52321810533d89c526c751b71a2dd91bc908",
				Sha256: "c37f93c73cf2f303f874c094f6f76e47b2421a3da9f0e7e0b98bea8a3d685322",
			}

			sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			sha256sum, err = util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("bash-prefetch-proxy"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=-100"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r -100 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "cbc6213119267fb7437947a971673a6a3eb65d3b972eac55b227104a4069f32c",
				Sha256: "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-100"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))

		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=0-100"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-100 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-0-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "ab59a8e67be5672729f97ecd6c9824561ab6e77652d59ea4e7b6eaf18a5a443e",
				Sha256: "7bbfcb694f6acc69483751d3b48d5bdbdcb284f9e04c7b7caa04b5977cffddc8",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-100"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100- -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "235c7f0377f3cea69659756c883243fd071ffb527c38d6abb407bf428ecc6489",
				Sha256: "ba9a10ceceb80562fc124dc9bc94ea2a38e3a71e3e746e2140e6381ac791cdeb",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-46026751"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-46026751 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "5135a54f35215fcbdfcea61313bc746ee4d2112781e7b346967aa53e3d266377",
				Sha256: "ba9a10ceceb80562fc124dc9bc94ea2a38e3a71e3e746e2140e6381ac791cdeb",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-46026751"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=0-46026751"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-46026751 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-0-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "c573851cdcabc1d5f7344c576ff67bdfe3f63f65b49e08942f24c15a1808a027",
				Sha256: "327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-46026751"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-10240"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-10240 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-10240' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-10240"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "cc826acc16c2ab33525874eb66baddb180709b33380983d489e510d6674ef6bb",
				Sha256: "8732360b941ad09a5e0e5d5f9891118bc068f6d0d5a56e3c6d483e4600fbc43f",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-10240"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})

	Context("/bin/kubectl file and set range header", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-1024"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-1024 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-1024' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-1024"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "60645e89632f754c704450108e2d188f7496cb63bb6055a5b1bf5c71f29d0b3a",
				Sha256: "d6d17dca18b8de59e38da525dc24c47b74fec1a790a9f64afdd6538f4f8fa90e",
			}

			sha256sum, err := util.CalculateSha256ByOutput([]*util.PodExec{seedClientPod}, util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-1024"))
			Expect(err).NotTo(HaveOccurred())
			Expect(fileMetadata.Sha256).To(Equal(sha256sum))

			time.Sleep(5 * time.Second)
			sha256sum, err = util.CalculateSha256ByTaskID([]*util.PodExec{seedClientPod}, fileMetadata.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect("327b4022d0bfd1d5e9c0701d4a3f989a536f7e6e865e102dcd77c7e7adb31f9a").To(Equal(sha256sum))
		})
	})
})
