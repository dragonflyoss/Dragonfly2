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

package urlutils

import (
	"net/url"

	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// FilterURLParam excludes queries in url with filters.
func FilterURLParam(str string, filters []string) string {
	if len(filters) == 0 || len(str) == 0 {
		return str
	}

	u, err := url.Parse(str)
	if err != nil {
		return ""
	}

	var values = make(url.Values)
	for k, v := range u.Query() {
		if !stringutils.ContainsFold(filters, k) {
			values[k] = v
		}
	}

	u.RawQuery = values.Encode()
	return u.String()
}

// IsValidURL returns whether the string url is a valid URL.
func IsValidURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
