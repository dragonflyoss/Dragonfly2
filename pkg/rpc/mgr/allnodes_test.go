/*
 *     Copyright 2020 The Dragonfly Authors
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

package mgr

import (
	"fmt"
	"github.com/serialx/hashring"
	"github.com/stretchr/testify/assert"
	"sort"
	"strconv"
	"testing"
)

func generateWeights(n int) map[string]int {
	result := make(map[string]int)
	for i := 0; i < n; i++ {
		result[fmt.Sprintf("%03d", i)] = i + 1
	}
	return result
}

func generateNodes(n int) []string {
	result := make([]string, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, fmt.Sprintf("%03d", i))
	}
	return result
}

func TestListOf1000Nodes(t *testing.T) {
	testData := map[string]struct {
		ring *hashring.HashRing
	}{
		"nodes":   {ring: hashring.New(generateNodes(1000))},
		"weights": {ring: hashring.NewWithWeights(generateWeights(1000))},
	}

	for testName, data := range testData {
		ring := data.ring
		t.Run(testName, func(t *testing.T) {
			nodes, ok := ring.GetNodes("key", ring.Size())
			assert.True(t, ok)
			if !assert.Equal(t, ring.Size(), len(nodes)) {
				// print debug info on failure
				sort.Strings(nodes)
				fmt.Printf("%v\n", nodes)
				return
			}

			// assert that each node shows up exatly once
			sort.Strings(nodes)
			for i, node := range nodes {
				actual, err := strconv.ParseInt(node, 10, 64)
				if !assert.NoError(t, err) {
					return
				}
				if !assert.Equal(t, int64(i), actual) {
					return
				}
			}
		})
	}
}
