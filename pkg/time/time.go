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

package time

import (
	"time"
)

// NanoToTime converts an int64 nanoseconds to a time
func NanoToTime(nsec int64) time.Time {
	return time.Unix(0, nsec)
}

// SubNano returns the difference between two nanoseconds
func SubNano(x int64, y int64) time.Duration {
	return NanoToTime(x).Sub(NanoToTime(y))
}
