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
	"context"
	"fmt"
	"os"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/searcher"
)

func main() {
	s, err := searcher.LoadPlugin("./testdata")
	if err != nil {
		fmt.Printf("load plugin error: %s\n", err)
		os.Exit(1)
	}

	clusters, err := s.FindSchedulerClusters(context.Background(), []models.SchedulerCluster{}, "127.0.0.1", "foo", map[string]string{}, logger.CoreLogger)
	if err != nil {
		fmt.Println("scheduler cluster not found")
		os.Exit(1)
	}

	if clusters[0].Name != "foo" {
		fmt.Println("scheduler cluster name wrong")
		os.Exit(1)
	}
}
