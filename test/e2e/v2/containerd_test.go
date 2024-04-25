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

var _ = Describe("Containerd with CRI support", func() {
	Context("dragonflyoss/manager:v2.0.0 image", func() {
		It("pull should be ok", Label("containerd", "pull"), func() {
			out, err := util.CriCtlCommand("pull", "dragonflyoss/manager:v2.1.0").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "ff679754c7951659b21b84fedc15c82e285909146329eae5114a762459b49221",
					Sha256: "ca51217de9012bffe54390f1a91365af22a06279a3f2b3e57d4d2dc99b989588",
				},
				{
					ID:     "cfe7b96cdd7af51b0433507da8c4239adb1985d27c8a62136b7ca4c2b0796d63",
					Sha256: "0d816dfc0753b877a04e3df93557bd3597fc7d0e308726655b14401c22a3b92a",
				},
				{
					ID:     "43ab6b508e9e57e990d38322572a39fd628fd5ec34d83ecd66aee5d199265f84",
					Sha256: "b5941d5a445040d3a792e5be361ca42989d97fc30ff53031f3004ccea8e44520",
				},
				{
					ID:     "88c81ef2a014504dd493eeb4ad140dbfe05e12a53038fea2f4f04e10fd4bcf38",
					Sha256: "c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
				},
				{
					ID:     "adad87ebcac1fc92d13051a8f15fc29f33f733a93ad322119e00764cbbbcb501",
					Sha256: "2a1bc4e0f20bb5ed9a2197ecffde7eace4a9b9179048614205d025df73ba97c7",
				},
				{
					ID:     "1dcf30a9df83b64fd8ad85288ed01def4d8758ee9c433861b86ceced46b2c97d",
					Sha256: "078ea4eebc352a499d7bb6ff65fab1325226e524acac89a9db922ad91cab88f1",
				},
			}

			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})

		It("rmi should be ok", Label("containerd", "rmi"), func() {
			out, err := util.CriCtlCommand("rmi", "dragonflyoss/manager:v2.1.0").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("dragonflyoss/scheduler:v2.0.0 image", func() {
		It("pull should be ok", Label("containerd", "pull"), func() {
			out, err := util.CriCtlCommand("pull", "dragonflyoss/scheduler:v2.0.0").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "69fa645056298e4809c88cf86c3fb559ffa62bb4d1f986cacf1aa81933e3d030",
					Sha256: "0f4277a6444fbaf4eb5a7f39103e281dd57969953c7425edc7c8d4aa419347eb",
				},
				{
					ID:     "23eaaf799cb4191256c352aec0a207089a26e105e779e7f93f3726598c975165",
					Sha256: "e55b67c1d5660c34dcb0d8e6923d0a50695a4f0d94f858353069bae17d0bfdea",
				},
				{
					ID:     "4dbaa1695f5410bcd59560cf79408c1ae69d27c8cfd94259359ff69e026ebdaa",
					Sha256: "8572bc8fb8a32061648dd183b2c0451c82be1bd053a4ea8fae991436b92faebb",
				},
				{
					ID:     "c65dac943d3f294a1e0005618ac197a4f8d38eb93176a631eba7e34913cb5747",
					Sha256: "88bfc12bad0cc91b2d47de4c7a755f6547b750256cc4c8b284e07aae13e4e041",
				},
			}

			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})

		It("rmi should be ok", Label("containerd", "rmi"), func() {
			out, err := util.CriCtlCommand("rmi", "dragonflyoss/scheduler:v2.0.0").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("dragonflyoss/client:v0.1.30 image", func() {
		It("pull should be ok", Label("containerd", "pull"), func() {
			out, err := util.CriCtlCommand("pull", "dragonflyoss/client:v0.1.30").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "d6998ea9e6f6d0fe8dfa5efe1098d4bc29234a298a1bf857c3129d534c064dd5",
					Sha256: "c8071d0de0f5bb17fde217dafdc9d2813ce9db77e60f6233bcd32f1c8888b121",
				},
				{
					ID:     "9764b091db2dca99e2a0c09c776be3915e913f02def63e794435e048d2cb7ad7",
					Sha256: "e964513726885fa2f977425fc889eabbe25c9fa47e7a4b0ec5e2baef96290f47",
				},
				{
					ID:     "e376fd3b1d92d8db1a29fc7aa307ae243eaa229a84ffc4f4f84247046fd75850",
					Sha256: "0e304933d7eae4674e05b3bc409f236c65077e2b7055119bbd66ff613fe5e1ad",
				},
				{
					ID:     "77fb97fa428921c5a5a72be510c69b1208db540479dda3a1ff765a248ed23287",
					Sha256: "53b01ef3d5d676a8514ded6b469932e33d84738e5e00932ca124382a8567c44b",
				},
				{
					ID:     "b58219de7051f520e222ce29c90e2d61b49649d714bd2d9e6ebcebbc83a15f2b",
					Sha256: "c9d959fc168ad8bdc9a021066eb9c1dd4de8e860c03619a88d8ba0ff5479d9ea",
				},
				{
					ID:     "4bceb8758fe687ac33610c23f503dc13d9050c15be3f20f9141b94a450070d9f",
					Sha256: "b6acfae843b58bf14369ebbeafa96af5352cde9a89f8255ca51f92b233a6e405",
				},
			}

			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})

		It("rmi should be ok", Label("containerd", "rmi"), func() {
			out, err := util.CriCtlCommand("rmi", "dragonflyoss/client:v0.1.30").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("dragonflyoss/dfinit:v0.1.30 image", func() {
		It("pull should be ok", Label("containerd", "pull"), func() {
			out, err := util.CriCtlCommand("pull", "dragonflyoss/dfinit:v0.1.30").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			taskMetadatas := []util.TaskMetadata{
				{
					ID:     "5f04fd58b821860c616ad3f8cfe096c53969086a99ec9d63a7b6d75e643cd33f",
					Sha256: "c58d97dd21c3b3121f262a1fbb5a278f77ab85dba7a02b819e710f34683cf746",
				},
				{
					ID:     "67948562926bcaafea035f5e7ae8007f1b367c5fb050ed57fc70faa1b95d73af",
					Sha256: "2ff0ae26fa61a2b0f88f470a8e50f7623ea48b224eb072a5878a20d663d5307d",
				},
				{
					ID:     "ea70b1749ce2e542271512ba2a50b01787b4a17edd42a2c1450097c60845a10c",
					Sha256: "b1826117441e607acd3b98c93cdb16759c2cc2240852055b8a2b5860f3204f1e",
				},
			}

			clientPod, err := util.ClientExec()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID([]*util.PodExec{clientPod}, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}

			time.Sleep(1 * time.Second)
			seedClientPods := make([]*util.PodExec, 3)
			for i := 0; i < 3; i++ {
				seedClientPods[i], err = util.SeedClientExec(i)
				fmt.Println(err)
				Expect(err).NotTo(HaveOccurred())
			}
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(seedClientPods, taskMetadata.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(taskMetadata.Sha256).To(Equal(sha256sum))
			}
		})

		It("rmi should be ok", Label("containerd", "rmi"), func() {
			out, err := util.CriCtlCommand("rmi", "dragonflyoss/dfinit:v0.1.30").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
