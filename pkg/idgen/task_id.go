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

	"d7y.io/dragonfly/v2/pkg/digest"
	neturl "d7y.io/dragonfly/v2/pkg/net/url"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

const (
	filterSeparator = "&"
)

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

// taskID generates a task id.
// filter is separated by & character.
func taskID(url string, meta *base.UrlMeta, ignoreRange bool) string {
	if meta == nil {
		return digest.Sha256(url)
	}

	filters := parseFilters(meta.Filter)

	var (
		u   string
		err error
	)
	u, err = neturl.FilterQuery(url, filters)
	if err != nil {
		u = ""
	}

	data := []string{u}
	if meta.Digest != "" {
		data = append(data, meta.Digest)
	}

	if !ignoreRange && meta.Range != "" {
		data = append(data, meta.Range)
	}

	if meta.Tag != "" {
		data = append(data, meta.Tag)
	}

	return digest.Sha256(data...)
}

// parseFilters parses a filter string to filter slice.
func parseFilters(rawFilters string) []string {
	if pkgstrings.IsBlank(rawFilters) {
		return nil
	}

	return strings.Split(rawFilters, filterSeparator)
}
