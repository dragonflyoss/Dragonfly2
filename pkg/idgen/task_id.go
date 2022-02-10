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

package idgen

import (
	"strings"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
)

func taskID(url string, meta *base.UrlMeta, ignoreRange bool) string {
	var filters []string
	if meta != nil && meta.Filter != "" {
		filters = strings.Split(meta.Filter, "&")
	}

	var data []string
	data = append(data, urlutils.FilterURLParam(url, filters))
	if meta != nil {
		if meta.Digest != "" {
			data = append(data, meta.Digest)
		}

		if !ignoreRange && meta.Range != "" {
			data = append(data, meta.Range)
		}

		if meta.Tag != "" {
			data = append(data, meta.Tag)
		}
	}

	return digestutils.Sha256(data...)
}

// TaskID generates a task id.
// filter is separated by & character.
func TaskID(url string, meta *base.UrlMeta) string {
	return taskID(url, meta, false)
}

// ParentTaskID generates a task id like TaskID, but without range.
// this func is used to check the parent tasks for ranged requests
func ParentTaskID(url string, meta *base.UrlMeta) string {
	return taskID(url, meta, true)
}
