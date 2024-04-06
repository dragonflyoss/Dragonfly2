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
	"os"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	"d7y.io/dragonfly/v2/test/e2e/util"
)

var _ = Describe("Evaluator with networkTopology", func() {
	Context("networkTopology", func() {
		It("check networkTopology in redis", Label("networkTopology"), func() {
			mode := os.Getenv("DRAGONFLY_COMPATIBILITY_E2E_TEST_MODE")
			if mode == schedulerCompatibilityTestMode {
				fmt.Println("networkTopology is disable, skip")
				return
			}
			Expect(waitForProbedInNetworkTopology()).Should(BeTrue())

			if waitForProbedInNetworkTopology() == true {
				time.Sleep(2 * time.Minute)
				Expect(checkNetworkTopologyUpdated()).Should(BeTrue())
			}
		})
	})
})

// getRedisExec get redis pod.
func getRedisExec() *util.PodExec {
	out, err := util.KubeCtlCommand("-n", dragonflyNamespace, "get", "pod", "-l", "app.kubernetes.io/name=redis",
		"-o", "jsonpath='{range .items[0]}{.metadata.name}{end}'").CombinedOutput()
	podName := strings.Trim(string(out), "'")
	Expect(err).NotTo(HaveOccurred())
	fmt.Println(podName)
	Expect(strings.HasPrefix(podName, "dragonfly-redis")).Should(BeTrue())
	return util.NewPodExec(dragonflyNamespace, podName, "redis")
}

func waitForProbedInNetworkTopology() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	redisPod := getRedisExec()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			out, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "dbsize").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			key, err := strconv.Atoi(strings.Split(string(out), "\n")[1])
			if key == 0 || err != nil {
				continue
			}

			out, err = redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "KEYS", "scheduler:network-topology:*").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			networkTopologyKey := strings.Split(string(out), "\n")[1]
			if networkTopologyKey == "" || err != nil {
				continue
			}
			networkTopologyOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "HGETALL", networkTopologyKey).CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			if networkTopologyOut == nil || err != nil {
				continue
			}

			out, err = redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "KEYS", "scheduler:probes:*").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			probesKey := strings.Split(string(out), "\n")[1]
			if probesKey == "" || err != nil {
				continue
			}
			probesOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "LRANGE", probesKey, "0", "-1").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			if probesOut == nil || err != nil {
				continue
			}

			out, err = redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "KEYS", "scheduler:probed-count:*").CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			probedCountKey := strings.Split(string(out), "\n")[1]
			if probedCountKey == "" || err != nil {
				continue
			}
			probedCountOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "GET", probedCountKey).CombinedOutput()
			Expect(err).NotTo(HaveOccurred())
			if probedCountOut == nil || err != nil {
				continue
			}

			return true
		}
	}
}

func checkNetworkTopologyUpdated() bool {
	redisPod := getRedisExec()

	var networkTopologyKey string
	out, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "KEYS", "scheduler:network-topology:*").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	for i := 1; i <= 3; i++ {
		networkTopologyKey = strings.Split(string(out), "\n")[i]
		updatedAtOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "HGET", networkTopologyKey, "updatedAt").CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		createdAtOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "HGET", networkTopologyKey, "createdAt").CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		if strings.Split(string(updatedAtOut), "\n")[1] == strings.Split(string(createdAtOut), "\n")[1] {
			return false
		}
	}

	var probedCountKey string
	out, err = redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "KEYS", "scheduler:probed-count:*").CombinedOutput()
	Expect(err).NotTo(HaveOccurred())
	for i := 1; i <= 3; i++ {
		probedCountKey = strings.Split(string(out), "\n")[i]
		probedCountOut, err := redisPod.Command("redis-cli", "-a", "dragonfly", "-n", "3", "GET", probedCountKey).CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		probedCount, err := strconv.Atoi(strings.Split(string(probedCountOut), "\n")[1])
		Expect(err).NotTo(HaveOccurred())
		if probedCount <= 1 && probedCount >= 50 {
			return false
		}
	}

	return true
}
