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

// FilterQueryParams filters the query params in the url.
func FilterQueryParams(rawURL string, filteredQueryParams []string) (string, error) {
	if len(filteredQueryParams) == 0 {
		return rawURL, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	hidden := make(map[string]struct{})
	for _, filter := range filteredQueryParams {
		hidden[filter] = struct{}{}
	}

	var values = make(url.Values)
	for k, v := range u.Query() {
		if _, ok := hidden[k]; !ok {
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
