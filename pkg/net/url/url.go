/*
 *     Copyright 2022 The Dragonfly Authors
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

package url

import (
	"net/url"
)

// FilterQuery excludes query string in url with filters.
func FilterQuery(rawURL string, filters []string) (string, error) {
	if len(filters) == 0 {
		return rawURL, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	var values = make(url.Values)
	for k, v := range u.Query() {
		var isFilter bool
		for _, filter := range filters {
			if k == filter {
				isFilter = true
				break
			}
		}

		if !isFilter {
			values[k] = v
		}
	}

	u.RawQuery = values.Encode()
	return u.String(), nil
}

// IsValid returns whether the string url is a valid URL.
func IsValid(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
