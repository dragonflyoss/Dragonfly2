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
				ID:     "2203d73a8d0162a86745eb25095a655e2ab866c56c3ebcc3f0bf7d6a4e329317",
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
				ID:     "6b94aa169d11ba67c1852f14c4df39e5ebf84d1c0d7a6f6ad0ba2f3547883f4f",
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
				ID:     "d6636c250217d958ef2377ca9278811cc3c538f3d29a65729ec7e73bbabf5bb6",
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
				ID:     "9c9754927a9b3be5b6a0524c3c6f930a292370d0c29f0617a4b54e94622b330b",
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
				ID:     "91623b36b06c8fe3c87af420b92df060884a1fda1b11951349a0f1d02f9cc2e3",
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

	Context("/bin/kubectl file and set range header bytes=-100", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=-100"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r -100 -H 'X-Dragonfly-Tag: proxy-bytes-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "5a7a62911a30fbcdf602b3b37ce3ecb56f4c47ab65283219c5d2342223ee53cb",
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

	Context("/bin/kubectl file and set range header bytes=0-100", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=0-100"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-100 -H 'X-Dragonfly-Tag: proxy-bytes-0-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-0-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "41d7764fb682932aac4e1591ff01739493539e6af9072773624b3e20fe5465f4",
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

	Context("/bin/kubectl file and set range header bytes=100-", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100- -H 'X-Dragonfly-Tag: proxy-bytes-100-' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "c29e8ee77bf3a0f177269fd1a216a4c404a4fb3778a483c41c8735aadffcbe66",
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

	Context("/bin/kubectl file and set range header bytes=100-46026751", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-46026751"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-46026751 -H 'X-Dragonfly-Tag: proxy-bytes-100-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "29fe25686d520e3aedd1743fc95dc008e6102e917b33d17b75b2db204718f132",
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

	Context("/bin/kubectl file and set range header bytes=0-46026751", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=0-46026751"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-46026751 -H 'X-Dragonfly-Tag: proxy-bytes-0-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-0-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "09f0107339e32f9405b15fb093b25db9652104d96d3a8797afffa47f5c4c988b",
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

	Context("/bin/kubectl file and set range header bytes=100-10240", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-10240"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-10240 -H 'X-Dragonfly-Tag: proxy-bytes-100-10240' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-10240"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "7a916912f2182f749d2779ad9c557947d7ee2a2b213705038d2441bf5089f5d0",
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

	Context("/bin/kubectl file and set range header bytes=100-1024", func() {
		It("download should be ok", Label("proxy", "download", "range: bytes=100-1024"), func() {
			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := clientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-1024 -H 'X-Dragonfly-Tag: proxy-bytes-100-1024' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-proxy-bytes-100-1024"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "7a916912f2182f749d2779ad9c557947d7ee2a2b213705038d2441bf5089f5d0",
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
				ID:     "09d0b6f7a9d6825d4d703b85ec706a87334ba437310301e4a0110af8cc562648",
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
				ID:     "0812fe2767598f19d2430be8153522d916f6e6c3179bd8914941eb2c0e8568e5",
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
				ID:     "d93cf0cd379fbd59194c3038837a551a1151a3aedd1c931fc7324ea0b0edc289",
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
				ID:     "ba68b1b385b2441fef06fb79a64b79367fabf4c31e2d7f21cd37b202d2e02e44",
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
				ID:     "661e488117503f6c46192e86f587a5b615129cadb6f25b51b9683257a1d70b55",
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

	Context("/bin/kubectl file and set range header bytes=-100", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=-100"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r -100 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "43a914f8b0f3ab0be61ee007b4246de0043612a691eb39e07efbcbb2b11be8e5",
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

	Context("/bin/kubectl file and set range header bytes=0-100", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=0-100"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-100 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-0-100' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-100"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "eb46a424abb2151d4d1337d030f3d15b510cf7827a45a0e033e81ed3d15d8743",
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

	Context("/bin/kubectl file and set range header bytes=100-", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100- -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "148e03a3ba5c45269de90465cff82d003e52a7ec7e3a2ea6fe08e45f4fb6fbdd",
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

	Context("/bin/kubectl file and set range header bytes=100-46026751", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-46026751"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-46026751 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "daf131380b88dd60a81140f87d36767cbfadc314324b71e0ee2a35679cf864b5",
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

	Context("/bin/kubectl file and set range header bytes=0-46026751", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=0-46026751"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 0-46026751 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-0-46026751' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-0-46026751"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "90f143ba29e0174095ca2e59a76d86f3acaa4072aec3263c2bacf51259eaece0",
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

	Context("/bin/kubectl file and set range header bytes=100-10240", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-10240"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-10240 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-10240' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-10240"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "52c748b962167e66421259a7834363b3956b1a3c5f8cea5575a1f96dae4be47d",
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

	Context("/bin/kubectl file and set range header bytes=100-1024", func() {
		It("download should be ok", Label("prefetch-proxy", "download", "range: bytes=100-1024"), func() {
			seedClientPod, err := util.SeedClientExec(0)
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			out, err := seedClientPod.Command("sh", "-c", fmt.Sprintf("curl -x 127.0.0.1:4001 -r 100-1024 -H 'X-Dragonfly-Tag: prefetch-proxy-bytes-100-1024' %s --output %s", util.GetFileURL("/bin/kubectl"), util.GetOutputPath("kubectl-prefetch-proxy-bytes-100-1024"))).CombinedOutput()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			fmt.Println(string(out))

			fileMetadata := util.FileMetadata{
				ID:     "52c748b962167e66421259a7834363b3956b1a3c5f8cea5575a1f96dae4be47d",
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
