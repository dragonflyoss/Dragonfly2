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

package test

import (
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/test/mock_client"
	. "github.com/onsi/ginkgo"
	"time"
)

var _ = Describe("One Client Download Test", func() {
	tl := common.NewE2ELogger()

	var (
		client *mock_client.MockClient
	)

	Describe("One Client Download a file Test", func() {
		It("should be download a file successfully", func() {
			client = mock_client.NewMockClient("127.0.0.1:8002", "http://dragonfly.com?type=single", "s", tl)
			go client.Start()
			stopCh := client.GetStopChan()
			select {
			case <-stopCh:
				tl.Log("client download file finished")
			case <-time.After(time.Minute):
				tl.Fatalf("download file failed")
			}
		})
	})
})
