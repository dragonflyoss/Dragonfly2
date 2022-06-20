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
	"testing"
	"time"
)

func TestNanoToTime(t *testing.T) {
	const Layout = "2006-01-02 15:04:05"
	sample, _ := time.ParseInLocation(Layout, "2021-01-02 12:04:05", time.Local)
	now := time.Now()
	tests := []struct {
		name string
		args int64
		want time.Time
	}{
		{
			name: "convert an int64 nanosecond to a time",
			args: sample.UnixNano(),
			want: sample.Local(),
		},
		{
			name: "convert now",
			args: now.UnixNano(),
			want: time.Unix(0, now.UnixNano()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NanoToTime(tt.args); got != tt.want {
				t.Errorf("NanoToTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubNano(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		args []int64
		want int64
	}{
		{
			name: "nanoseconds are not equal",
			args: []int64{now.Add(1 * time.Nanosecond).UnixNano(), now.UnixNano()},
			want: 1,
		},
		{
			name: "nanoseconds are equal",
			args: []int64{now.UnixNano(), now.UnixNano()},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubNano(tt.args[0], tt.args[1]); got.Nanoseconds() != tt.want {
				t.Errorf("SubNano() = %v, want %v", got, tt.want)
			}
		})
	}
}
