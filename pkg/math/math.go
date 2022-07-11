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

package math

import "golang.org/x/exp/constraints"

// Max returns the maximum of values.
func Max[T constraints.Ordered](values ...T) T {
	max := values[0]
	for _, value := range values {
		if value > max {
			max = value
		}
	}

	return max
}

// Min returns the minimum of values.
func Min[T constraints.Ordered](values ...T) T {
	min := values[0]
	for _, value := range values {
		if value < min {
			min = value
		}
	}

	return min
}
