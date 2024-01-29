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
	"fmt"
	"strings"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	pkgdigest "d7y.io/dragonfly/v2/pkg/digest"
	neturl "d7y.io/dragonfly/v2/pkg/net/url"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

const (
	// FilteredQueryParamsSeparator is the separator of filtered query params.
	FilteredQueryParamsSeparator = "&"
)

// TaskIDV1 generates v1 version of task id.
// filter is separated by & character.
func TaskIDV1(url string, meta *commonv1.UrlMeta) string {
	return taskIDV1(url, meta, false)
}

// ParentTaskIDV1 generates v1 version of parent task id, but without range.
// this func is used to check the parent tasks for ranged requests
func ParentTaskIDV1(url string, meta *commonv1.UrlMeta) string {
	return taskIDV1(url, meta, true)
}

// taskIDV1 generates v1 version of task id.
// filter is separated by & character.
func taskIDV1(url string, meta *commonv1.UrlMeta, ignoreRange bool) string {
	if meta == nil {
		return pkgdigest.SHA256FromStrings(url)
	}

	filteredQueryParams := parseFilteredQueryParams(meta.Filter)

	var (
		u   string
		err error
	)
	u, err = neturl.FilterQueryParams(url, filteredQueryParams)
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

	if meta.Application != "" {
		data = append(data, meta.Application)
	}

	return pkgdigest.SHA256FromStrings(data...)
}

// parseFilteredQueryParams parses filtered query params.
func parseFilteredQueryParams(rawFilteredQueryParams string) []string {
	if pkgstrings.IsBlank(rawFilteredQueryParams) {
		return nil
	}

	return strings.Split(rawFilteredQueryParams, FilteredQueryParamsSeparator)
}

// TaskIDV2 generates v2 version of task id.
func TaskIDV2(url, digest, tag, application string, pieceLength int32, filteredQueryParams []string) string {
	url, err := neturl.FilterQueryParams(url, filteredQueryParams)
	if err != nil {
		url = ""
	}

	return pkgdigest.SHA256FromStrings(url, digest, tag, application, fmt.Sprint(pieceLength))
}
