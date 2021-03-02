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
	"fmt"

	"d7y.io/dragonfly/v2/pkg/idutils"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *SchedulerTestSuite) Test001TaskIdGenerate() {
	var testers = []struct {
		filter string
		url1   string
		mate1  *base.UrlMeta
		url2   string
		mate2  *base.UrlMeta
		expect bool
	}{{
		filter: "param2&param3",
		url1:   "http://dragonfly.com/file/test.txt?param1=3&param2=4",
		mate1:  nil,
		url2:   "http://dragonfly.com/file/test.txt?param1=3&param2=5",
		mate2:  nil,
		expect: true,
	}, {
		filter: "",
		url1:   "http://dragonfly.com/file/test.txt?param1=3&param2=4",
		mate1:  nil,
		url2:   "http://dragonfly.com/file/test.txt?param1=3&param2=5",
		mate2:  nil,
		expect: false,
	}, {
		filter: "",
		url1:   "http://dragonfly.com/file/test.txt?param1=3&param2=4",
		mate1:  &base.UrlMeta{Range: "0-100", Md5: "123"},
		url2:   "http://dragonfly.com/file/test.txt?param1=3&param2=5",
		mate2:  &base.UrlMeta{Range: "40-100", Md5: "456"},
		expect: false,
	}, {
		filter: "param2&param3",
		url1:   "http://dragonfly.com/file/test.txt?param1=3&param2=4",
		mate1:  &base.UrlMeta{Range: "0-100", Md5: "123"},
		url2:   "http://dragonfly.com/file/test.txt?param1=3&param2=5",
		mate2:  &base.UrlMeta{Range: "0-100", Md5: "123"},
		expect: true,
	}}

	for i, test := range testers {
		taskId1 := idgen.GenerateTaskId(test.url1, test.filter, test.mate1,"")
		taskId2 := idgen.GenerateTaskId(test.url2, test.filter, test.mate2,"")
		suite.Equal(test.expect, taskId1 == taskId2, fmt.Sprintf("generate task id test failed case[%d]", i))
	}
}
