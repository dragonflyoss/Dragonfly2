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
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/test/e2e/v2/util"
)

var _ = Describe("Clients Leaving", func() {
	Context("graceful exit", func() {
		It("number of hosts should be ok", Label("host", "leave"), func() {
			schedulerClient, err := schedulerclient.GetV2ByAddr(context.Background(), ":8002", grpc.WithTransportCredentials(insecure.NewCredentials()))
			Expect(err).NotTo(HaveOccurred())

			hostCount := util.Servers[util.ClientServerName].Replicas
			Expect(calculateNormalHostCountFromScheduler(schedulerClient)).To(Equal(hostCount))

			podName, err := util.GetClientPodNameInMaster()
			Expect(err).NotTo(HaveOccurred())

			// Add taint to master node to prevent new client from starting.
			out, err := util.KubeCtlCommand("-n", util.DragonflyNamespace, "taint", "nodes", "kind-control-plane", "e2e:NoSchedule").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Delete client pod in master, client will leave gracefully with cleanup.
			out, err = util.KubeCtlCommand("-n", util.DragonflyNamespace, "delete", "pod", podName, "--grace-period=30").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Wait fot the client to leave gracefully. The gc interval is 2 minutes.
			time.Sleep(2 * time.Minute)
			Expect(calculateNormalHostCountFromScheduler(schedulerClient)).To(Equal(hostCount - 1))

			// Remove taint in master node.
			out, err = util.KubeCtlCommand("-n", util.DragonflyNamespace, "taint", "nodes", "kind-control-plane", "e2e:NoSchedule-").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Wait for the client to start again.
			time.Sleep(1 * time.Minute)
		})
	})

	Context("force delete", func() {
		It("number of hosts should be ok", Label("host", "leave"), func() {
			schedulerClient, err := schedulerclient.GetV2ByAddr(context.Background(), ":8002", grpc.WithTransportCredentials(insecure.NewCredentials()))
			Expect(err).NotTo(HaveOccurred())

			hostCount := util.Servers[util.ClientServerName].Replicas
			Expect(calculateNormalHostCountFromScheduler(schedulerClient)).To(Equal(hostCount))

			podName, err := util.GetClientPodNameInMaster()
			Expect(err).NotTo(HaveOccurred())

			// Add taint to master node to prevent new client from starting.
			out, err := util.KubeCtlCommand("-n", util.DragonflyNamespace, "taint", "nodes", "kind-control-plane", "e2e:NoSchedule").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Force delete client pod in master, client will leave without cleanup.
			out, err = util.KubeCtlCommand("-n", util.DragonflyNamespace, "delete", "pod", podName, "--force", "--grace-period=0").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Wait for host gc with interval 2 minute.
			time.Sleep(2 * time.Minute)
			Expect(calculateNormalHostCountFromScheduler(schedulerClient)).To(Equal(hostCount - 1))

			// Remove taint in master node.
			out, err = util.KubeCtlCommand("-n", util.DragonflyNamespace, "taint", "nodes", "kind-control-plane", "e2e:NoSchedule-").CombinedOutput()
			fmt.Println(string(out))
			Expect(err).NotTo(HaveOccurred())

			// Wait for the client to start again.
			time.Sleep(1 * time.Minute)
		})
	})
})

func calculateNormalHostCountFromScheduler(schedulerClient schedulerclient.V2) (hostCount int) {
	resp, err := schedulerClient.ListHosts(context.Background(), "")
	fmt.Println(resp, err)
	Expect(err).NotTo(HaveOccurred())

	for _, host := range resp.Hosts {
		hostType := types.HostType(host.Type)
		if hostType == types.HostTypeNormal {
			hostCount++
		}
	}

	return
}
