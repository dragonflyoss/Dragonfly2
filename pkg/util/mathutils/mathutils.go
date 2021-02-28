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

// Package mathutils provides utilities supplementing the standard 'math' and 'math/rand' packages.
package mathutils

import (
	"strconv"
)

func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}

	return b
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}

	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func IsNatural(value string) bool {
	if v, err := strconv.ParseInt(value, 0, 64); err == nil {
		return v >= 0
	}

	return false
}

func IsInteger(value string) bool {
	if _, err := strconv.ParseInt(value, 0, 64); err == nil {
		return true
	}

	return false
}
