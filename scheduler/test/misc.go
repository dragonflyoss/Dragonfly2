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
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/types"
	"d7y.io/dragonfly/v2/scheduler/test/common"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("Scheduler Misc Test", func() {
	tl := common.NewE2ELogger()

	Describe("Scheduler TaskId Generate Test", func() {
		It("should be Generate TaskId same", func() {
			filter := "param2&param3"
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, nil)
			taskId2 := types.GenerateTaskId(url2, filter, nil)
			if taskId1 != taskId2 {
				tl.Fatalf("taskId1[%s] != taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId different", func() {
			filter := ""
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, nil)
			taskId2 := types.GenerateTaskId(url2, filter, nil)
			if taskId1 == taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId different with UrlMeta", func() {
			filter := "param2&param3"
			meta1 := &base.UrlMeta{Range: "0-100", Md5: "123"}
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			meta2 := &base.UrlMeta{Range: "40-100", Md5: "456"}
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, meta1)
			taskId2 := types.GenerateTaskId(url2, filter, meta2)
			if taskId1 == taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})

		It("should be Generate TaskId same with UrlMeta", func() {
			filter := "param2&param3"
			meta1 := &base.UrlMeta{Range: "0-100", Md5: "123"}
			url1 := "http://dragonfly.com/file/test.txt?param1=3&param2=4"
			meta2 := &base.UrlMeta{Range: "0-100", Md5: "123"}
			url2 := "http://dragonfly.com/file/test.txt?param1=3&param2=5"
			taskId1 := types.GenerateTaskId(url1, filter, meta1)
			taskId2 := types.GenerateTaskId(url2, filter, meta2)
			if taskId1 != taskId2 {
				tl.Fatalf("taskId1[%s] == taskId2[%s]", taskId1, taskId2)
				return
			}
		})
	})
})
