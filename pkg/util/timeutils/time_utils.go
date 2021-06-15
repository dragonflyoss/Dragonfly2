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

// Package timeutils provides utilities supplementing the standard 'time' package.
package timeutils

import (
	"net/http"
	"time"
)

func CurrentTimeMillis() int64 {
	return time.Now().UnixNano() / time.Millisecond.Nanoseconds()
}

func SinceInMilliseconds(start time.Time) int64 {
	return time.Since(start).Nanoseconds() / time.Millisecond.Nanoseconds()
}

// UnixMillis converts a {Mon, 02 Jan 2006 15:04:05 GMT} time to an int64 milliseconds.
func UnixMillis(timeString string) int64 {
	t, err := time.ParseInLocation(http.TimeFormat, timeString, time.UTC)
	if err != nil {
		return 0
	}

	return t.UnixNano() / time.Millisecond.Nanoseconds()
}

// MillisUnixTime converts an int64 milliseconds to a unixTime
func MillisUnixTime(millis int64) time.Time {
	return time.Unix(millis/1000, millis%1000*int64(time.Millisecond))
}
