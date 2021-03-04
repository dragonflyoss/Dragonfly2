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

// ConvertTimeStringToInt converts a string time to an int64 timestamp.
func ConvertTimeStringToInt(timeStr string) (int64, error) {
	formatTime, err := time.ParseInLocation(http.TimeFormat, timeStr, time.UTC)
	if err != nil {
		return 0, err
	}

	return formatTime.Unix() * int64(1000), nil
}
