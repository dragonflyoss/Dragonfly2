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
					ID:     "c6c9f8b1ded96e4355f46c56c858cc52906ece01df55ad2c9600bd3635af2b74",
					Sha256: "ca51217de9012bffe54390f1a91365af22a06279a3f2b3e57d4d2dc99b989588",
				},
				{
					ID:     "389a7dd948c14ecaa3b36d0c3697a41b2547ea556ca0f1b427860b4b11e80815",
					Sha256: "0d816dfc0753b877a04e3df93557bd3597fc7d0e308726655b14401c22a3b92a",
				},
				{
					ID:     "d16441e8c63799b5148a46edb0af535dd017f43f684510bd5ddd358e901c36ad",
					Sha256: "b5941d5a445040d3a792e5be361ca42989d97fc30ff53031f3004ccea8e44520",
				},
				{
					ID:     "221de8465d2047153514e30cd586b9a6398bc8860ad5ea7c5853a3ca09b423c2",
					Sha256: "c1d6d1b2d5a367259e6e51a7f4d1ccd66a28cc9940d6599d8a8ea9544dd4b4a8",
				},
				{
					ID:     "71f89a34789a43c3ad7f55842890d0caf5a45a833bf4c8b72874b174f0358127",
					Sha256: "2a1bc4e0f20bb5ed9a2197ecffde7eace4a9b9179048614205d025df73ba97c7",
				},
				{
					ID:     "166359883b2334eb1ee9a9d49209a4286328c5e28605367e2d073fbdac6f5d0d",
					Sha256: "078ea4eebc352a499d7bb6ff65fab1325226e524acac89a9db922ad91cab88f1",
				},
			}

			clientPods, err := util.ClientExecAll()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())
			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(clientPods, taskMetadata.ID)
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
					ID:     "b01e905eb9803c65ade52d10b56cd024d5437377a477a53488cbbd346e895918",
					Sha256: "0f4277a6444fbaf4eb5a7f39103e281dd57969953c7425edc7c8d4aa419347eb",
				},
				{
					ID:     "f318e52bbbd81e2ba2afc697379118445ea32df733c92b20bfab6ba5183257a4",
					Sha256: "e55b67c1d5660c34dcb0d8e6923d0a50695a4f0d94f858353069bae17d0bfdea",
				},
				{
					ID:     "5276a8a1dc433233d58c36a16a795511c946cf1f27b36ec60efeaae43a615b23",
					Sha256: "8572bc8fb8a32061648dd183b2c0451c82be1bd053a4ea8fae991436b92faebb",
				},
				{
					ID:     "83f9961825895cb4890a15db88980832adf825070c148d4260dfeb703652cd7f",
					Sha256: "88bfc12bad0cc91b2d47de4c7a755f6547b750256cc4c8b284e07aae13e4e041",
				},
			}

			clientPods, err := util.ClientExecAll()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(clientPods, taskMetadata.ID)
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
					ID:     "157855128cf9d03cf9d5dcc53ec5d13a1dd26b252c6eda628fec21ccf9a8c349",
					Sha256: "c8071d0de0f5bb17fde217dafdc9d2813ce9db77e60f6233bcd32f1c8888b121",
				},
				{
					ID:     "76773bad88ce3fded570cc165fe9c11b946622a07172727e18f64fb4316efc5a",
					Sha256: "e964513726885fa2f977425fc889eabbe25c9fa47e7a4b0ec5e2baef96290f47",
				},
				{
					ID:     "c65a3d75938ab15d2520c6ed8a8680ae41921172f0c9359b6ed7e67573f7989d",
					Sha256: "0e304933d7eae4674e05b3bc409f236c65077e2b7055119bbd66ff613fe5e1ad",
				},
				{
					ID:     "d96e650ebf7ec07618a8629880ca0da62fd9e36a9fe59eb309636c6c66410489",
					Sha256: "53b01ef3d5d676a8514ded6b469932e33d84738e5e00932ca124382a8567c44b",
				},
				{
					ID:     "666b5bdb2a3c0498dfb3b73b95bc85b2679d0c65ef5d8e80ae9038b92631bfec",
					Sha256: "c9d959fc168ad8bdc9a021066eb9c1dd4de8e860c03619a88d8ba0ff5479d9ea",
				},
				{
					ID:     "886aaa587415010d418ce8d3848fda6c3c0d2c8ae733960d28e2bc9149e5ed07",
					Sha256: "b6acfae843b58bf14369ebbeafa96af5352cde9a89f8255ca51f92b233a6e405",
				},
			}

			clientPods, err := util.ClientExecAll()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(clientPods, taskMetadata.ID)
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
					ID:     "3a909585362079e870dd1f2286433b24fe1f2007599dadc6eaf5f73d8ba2b209",
					Sha256: "c58d97dd21c3b3121f262a1fbb5a278f77ab85dba7a02b819e710f34683cf746",
				},
				{
					ID:     "843ad9a841e99d9e8c18e5cccc200ed81e825bcecbeb749f6956bb7e43db0bfc",
					Sha256: "2ff0ae26fa61a2b0f88f470a8e50f7623ea48b224eb072a5878a20d663d5307d",
				},
				{
					ID:     "89d8de8f4cabeba625ffe416425a4c821e327b8e687096351bbf072b34018a19",
					Sha256: "b1826117441e607acd3b98c93cdb16759c2cc2240852055b8a2b5860f3204f1e",
				},
			}

			clientPods, err := util.ClientExecAll()
			fmt.Println(err)
			Expect(err).NotTo(HaveOccurred())

			for _, taskMetadata := range taskMetadatas {
				sha256sum, err := util.CalculateSha256ByTaskID(clientPods, taskMetadata.ID)
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
