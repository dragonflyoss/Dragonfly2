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

package strings

import (
	"strings"
)

// IsBlank determines whether the string is empty.
func IsBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

// Contains reports whether the string contains the element.
func Contains(slice []string, ele string) bool {
	for _, one := range slice {
		if one == ele {
			return true
		}
	}

	return false
}

// Remove the duplicate elements in the string slice.
func Unique(slice []string) []string {
	keys := make(map[string]bool)
	result := []string{}
	for _, entry := range slice {
		if _, ok := keys[entry]; !ok {
			keys[entry] = true
			result = append(result, entry)
		}
	}

	return result
}
