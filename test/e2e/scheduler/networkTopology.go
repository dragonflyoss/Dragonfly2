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

package scheduler

import (
	"context"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo/v2" //nolint
	. "github.com/onsi/gomega"    //nolint

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
)

var _ = Describe("Evaluator with networkTopology", func() {
	Context("networkTopology", func() {
		It("check networkTopology in redis", Label("networkTopology"), func() {
			Expect(waitForProbedInNetworkTopology()).Should(BeTrue())
		})
	})
})

func newRedis() (redis.UniversalClient, error) {
	rdb, err := pkgredis.NewRedis(&redis.UniversalOptions{
		Addrs:      []string{redisMaster},
		MasterName: redisMaster,
		DB:         networkTopologyDB,
		Username:   redisUsername,
		Password:   redisPassword,
	})
	if err != nil {
		return nil, err
	}

	return rdb, nil
}

func waitForProbedInNetworkTopology() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	rdb, err := newRedis()
	if err != nil {
		return false
	}

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			keys, err := rdb.Keys(context.Background(), "*").Result()
			Expect(err).NotTo(HaveOccurred())
			if len(keys) == 0 {
				continue
			}

			probesKeys, _, err := rdb.Scan(ctx, 0, pkgredis.MakeProbesKeyInScheduler("*", "*"), math.MaxInt64).Result()
			Expect(err).NotTo(HaveOccurred())
			if len(probesKeys) == 0 {
				continue
			}

			probedCountKeys, _, err := rdb.Scan(ctx, 0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).Result()
			Expect(err).NotTo(HaveOccurred())
			if len(probedCountKeys) == 0 {
				continue
			}

			networkTopologyKeys, _, err := rdb.Scan(ctx, 0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", "*"), math.MaxInt64).Result()
			Expect(err).NotTo(HaveOccurred())
			if len(networkTopologyKeys) == 0 {
				continue
			}

			return true
		}
	}
}
