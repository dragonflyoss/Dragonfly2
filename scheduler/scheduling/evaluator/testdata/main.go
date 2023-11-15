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

package main

import (
	"fmt"
	"os"

	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling/evaluator"
)

func main() {
	e, err := evaluator.LoadPlugin("./testdata")
	if err != nil {
		fmt.Printf("load plugin error: %s\n", err)
		os.Exit(1)
	}

	candidateParents := e.EvaluateParents([]*resource.Peer{&resource.Peer{}}, &resource.Peer{}, int32(0))
	if len(candidateParents) != 1 {
		fmt.Println("Evaluate failed")
		os.Exit(1)
	}

	if ok := e.IsBadNode(&resource.Peer{}); !ok {
		fmt.Println("IsBadNode failed")
		os.Exit(1)
	}
}
