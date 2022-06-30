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

package util

import (
	"testing"
)

var ParseRangeTests = []struct {
	s      string
	length int64
	r      []Range
}{
	{"", 0, nil},
	{"", 1000, nil},
	{"foo", 0, nil},
	{"bytes=", 0, nil},
	{"bytes=7", 10, nil},
	{"bytes= 7 ", 10, nil},
	{"bytes=1-", 0, nil},
	{"bytes=5-4", 10, nil},
	{"bytes=0-2,5-4", 10, nil},
	{"bytes=2-5,4-3", 10, nil},
	{"bytes=--5,4--3", 10, nil},
	{"bytes=A-", 10, nil},
	{"bytes=A- ", 10, nil},
	{"bytes=A-Z", 10, nil},
	{"bytes= -Z", 10, nil},
	{"bytes=5-Z", 10, nil},
	{"bytes=Ran-dom, garbage", 10, nil},
	{"bytes=0x01-0x02", 10, nil},
	{"bytes=         ", 10, nil},
	{"bytes= , , ,   ", 10, nil},

	{"bytes=0-9", 10, []Range{{0, 10}}},
	{"bytes=0-", 10, []Range{{0, 10}}},
	{"bytes=5-", 10, []Range{{5, 5}}},
	{"bytes=0-20", 10, []Range{{0, 10}}},
	{"bytes=15-,0-5", 10, []Range{{0, 6}}},
	{"bytes=1-2,5-", 10, []Range{{1, 2}, {5, 5}}},
	{"bytes=-2 , 7-", 11, []Range{{9, 2}, {7, 4}}},
	{"bytes=0-0 ,2-2, 7-", 11, []Range{{0, 1}, {2, 1}, {7, 4}}},
	{"bytes=-5", 10, []Range{{5, 5}}},
	{"bytes=-15", 10, []Range{{0, 10}}},
	{"bytes=0-499", 10000, []Range{{0, 500}}},
	{"bytes=500-999", 10000, []Range{{500, 500}}},
	{"bytes=-500", 10000, []Range{{9500, 500}}},
	{"bytes=9500-", 10000, []Range{{9500, 500}}},
	{"bytes=0-0,-1", 10000, []Range{{0, 1}, {9999, 1}}},
	{"bytes=500-600,601-999", 10000, []Range{{500, 101}, {601, 399}}},
	{"bytes=500-700,601-999", 10000, []Range{{500, 201}, {601, 399}}},

	// Match Apache laxity:
	{"bytes=   1 -2   ,  4- 5, 7 - 8 , ,,", 11, []Range{{1, 2}, {4, 2}, {7, 2}}},
}

func TestParseRange(t *testing.T) {
	for _, test := range ParseRangeTests {
		r := test.r
		ranges, err := ParseRange(test.s, test.length)
		if err != nil && r != nil {
			t.Errorf("ParseRange(%q) returned error %q", test.s, err)
		}
		if len(ranges) != len(r) {
			t.Errorf("len(ParseRange(%q)) = %d, want %d", test.s, len(ranges), len(r))
			continue
		}
		for i := range r {
			if ranges[i].Start != r[i].Start {
				t.Errorf("ParseRange(%q)[%d].Serve = %d, want %d", test.s, i, ranges[i].Start, r[i].Start)
			}
			if ranges[i].Length != r[i].Length {
				t.Errorf("ParseRange(%q)[%d].Length = %d, want %d", test.s, i, ranges[i].Length, r[i].Length)
			}
		}
	}
}

func TestGetRange(t *testing.T) {
	if GetContentRange(100, 200, -1) != "bytes 100-200/*" {
		t.Fail()
	}
	if GetContentRange(100, 200, 500) != "bytes 100-200/500" {
		t.Fail()
	}
}
