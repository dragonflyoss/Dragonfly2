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
					ID:     "dda3df5ddf512a650e3bd465647f93a52f7f636012ca12bd7ab33906245076e7",
					Sha256: "ca51217de9012bffe54390f1a91365af22a06279a3f2b3e57d4d2dc99b989588",
				},
				{
					ID:     "384bd63e76fb2af034f44de7c2b1377eb3ad161ab75b0af1de73189b6c274035",
					Sha256: "0d816dfc0753b877a04e3df93557bd3597fc7d0e308726655b14401c22a3b92a",
				},
				{
					ID:     "d72baec2558ad1cdc53e00de5d98e2ed403d24d5ced55244e2ac6dec6db9b6e9",
					Sha256: "b5941d5a445040d3a792e5be361ca42989d97fc30ff53031f3004ccea8e44520",
				},
				{
					ID:     "848a2731dce0f7e89255b95a4aee74431c0e306eb6ce968a5a70be5d3a707a8b",
					Sha256: "c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
				},
				{
					ID:     "f89ed6841a98697e6e6379f27f5b43abb9546a7200ca908d0d085e3a1d9593f9",
					Sha256: "2a1bc4e0f20bb5ed9a2197ecffde7eace4a9b9179048614205d025df73ba97c7",
				},
				{
					ID:     "02061e017949a88360a3ba2794c4e95198f80c737cc35ec105f1f0a4da67fa8c",
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
					ID:     "bd2e2f7cbe2c6649214d62dc53b30f8ed1251366e0dc6422e25ef5c6a6a2bd60",
					Sha256: "0f4277a6444fbaf4eb5a7f39103e281dd57969953c7425edc7c8d4aa419347eb",
				},
				{
					ID:     "23f48255ac61646aed5bbecc706ca539a7964e2128ff01911e492de654a63863",
					Sha256: "e55b67c1d5660c34dcb0d8e6923d0a50695a4f0d94f858353069bae17d0bfdea",
				},
				{
					ID:     "0d5a5e975c20d654e79c468e6ffd94d647baabf62df73b594adcfeee3161c10f",
					Sha256: "8572bc8fb8a32061648dd183b2c0451c82be1bd053a4ea8fae991436b92faebb",
				},
				{
					ID:     "45003cfb0ebaa79c19e5d8cf5950aace1b7595e10fae0ee2b71c5481f7fbbd2d",
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
					ID:     "e7eb414a034771e5347c75d6414e87263a823fdcff9d125a06c357b19411d615",
					Sha256: "c8071d0de0f5bb17fde217dafdc9d2813ce9db77e60f6233bcd32f1c8888b121",
				},
				{
					ID:     "999ea8db23bd6a89ace9e423b5780196a1856212edce12eb44d56a5573ce2ff5",
					Sha256: "e964513726885fa2f977425fc889eabbe25c9fa47e7a4b0ec5e2baef96290f47",
				},
				{
					ID:     "62e9b2b958f28f12f3ac8710dc4fab576268008e6115759e5dbd263e7a4afba3",
					Sha256: "0e304933d7eae4674e05b3bc409f236c65077e2b7055119bbd66ff613fe5e1ad",
				},
				{
					ID:     "a96b36f50370a5dbed258f13fd84b216b99992f49520643017b2845adca784ea",
					Sha256: "53b01ef3d5d676a8514ded6b469932e33d84738e5e00932ca124382a8567c44b",
				},
				{
					ID:     "f314d50106ab72ca9f3d1ebbbf4a71bcb0efb861a1efc70cb707a4204df52250",
					Sha256: "c9d959fc168ad8bdc9a021066eb9c1dd4de8e860c03619a88d8ba0ff5479d9ea",
				},
				{
					ID:     "9df82cfbd39d9956071967f8ef4b04f50674d01cc835bcc8e9f572c484ab3c39",
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
					ID:     "b08c8967e69896dda94b814d56c4154d8599c4660349e3722de01b214cbb122e",
					Sha256: "c58d97dd21c3b3121f262a1fbb5a278f77ab85dba7a02b819e710f34683cf746",
				},
				{
					ID:     "48f0dc8525c8652c6b9910c617367ca35d796880626c4044534ca35724f11a53",
					Sha256: "2ff0ae26fa61a2b0f88f470a8e50f7623ea48b224eb072a5878a20d663d5307d",
				},
				{
					ID:     "c652a1dd59249ca4f7eff3a61f4fdd5cb7c17215fe721a9ed6e105acf34bc6d9",
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
