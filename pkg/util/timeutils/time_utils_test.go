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

package timeutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCurrentTimeMillis(t *testing.T) {
	v1 := CurrentTimeMillis()
	time.Sleep(time.Millisecond * 500)
	v2 := CurrentTimeMillis()
	assert.LessOrEqual(t, v1, v2)
}

func TestSinceInMilliseconds(t *testing.T) {
	tim := time.Now()
	time.Sleep(500 * time.Millisecond)

	assert.GreaterOrEqual(t, SinceInMilliseconds(tim), int64(500))
}

func TestUnixMillis(t *testing.T) {
	const Layout = "Mon, 02 Jan 2006 15:04:05 GMT"
	sample, _ := time.Parse(Layout, "Mon, 02 Jan 2006 15:04:05 GMT")
	type args struct {
		timeString string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "convert a string time to an int64 milliseconds",
			args: args{"Mon, 02 Jan 2006 15:04:05 GMT"},
			want: sample.Unix() * 1000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UnixMillis(tt.args.timeString); got != tt.want {
				t.Errorf("UnixMillis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnixSeconds(t *testing.T) {
	const Layout = "Mon, 02 Jan 2006 15:04:05 GMT"
	sample, _ := time.Parse(Layout, "Mon, 02 Jan 2006 15:04:05 GMT")
	type args struct {
		timeString string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "convert a string time to an int64 milliseconds",
			args: args{"Mon, 02 Jan 2006 15:04:05 GMT"},
			want: sample.Unix(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UnixSeconds(tt.args.timeString); got != tt.want {
				t.Errorf("UnixMillis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMillisUnixTime(t *testing.T) {
	const Layout = "2006-01-02 15:04:05"
	sample, _ := time.ParseInLocation(Layout, "2021-01-02 12:04:05", time.Local)
	tests := []struct {
		name string
		args int64
		want time.Time
	}{
		{
			name: "convert an int64 milliseconds to a unix time",
			args: sample.Unix() * 1000,
			want: sample.Local(),
		},
		{
			name: "convert now",
			args: time.Now().UnixNano() / int64(time.Millisecond),
			want: time.Unix(time.Now().Unix(), (time.Now().UnixNano()-time.Now().Unix()*int64(time.Second))/int64(time.Millisecond)*int64(time.Millisecond)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MillisUnixTime(tt.args); got != tt.want {
				t.Errorf("MillisUnixTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecondsUnixTime(t *testing.T) {
	const Layout = "2006-01-02 15:04:05"
	sample, _ := time.ParseInLocation(Layout, "2021-01-02 12:04:05", time.Local)
	tests := []struct {
		name string
		args int64
		want time.Time
	}{
		{
			name: "convert an int64 seconds to a unix time",
			args: sample.Unix(),
			want: sample.Local(),
		},
		{
			name: "convert now",
			args: time.Now().Unix(),
			want: time.Unix(time.Now().Unix(), 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SecondsUnixTime(tt.args); got != tt.want {
				t.Errorf("MillisUnixTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
