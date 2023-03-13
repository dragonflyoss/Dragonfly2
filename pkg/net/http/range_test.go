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

package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRange_String(t *testing.T) {
	tests := []struct {
		s      string
		rg     Range
		expect func(t *testing.T, s string)
	}{
		{
			s: "bytes=0-9",
			rg: Range{
				Start:  0,
				Length: 10,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "bytes=0-9")
			},
		},
		{
			s: "bytes=1-10",
			rg: Range{
				Start:  1,
				Length: 10,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "bytes=1-10")
			},
		},
		{
			s: "bytes=1-0",
			rg: Range{
				Start:  1,
				Length: 0,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "bytes=1-0")
			},
		},
		{
			s: "bytes=1-1",
			rg: Range{
				Start:  1,
				Length: 1,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "bytes=1-1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			tc.expect(t, tc.rg.String())
		})
	}
}

func TestRange_URLMetaString(t *testing.T) {
	tests := []struct {
		s      string
		rg     Range
		expect func(t *testing.T, s string)
	}{
		{
			s: "0-9",
			rg: Range{
				Start:  0,
				Length: 10,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "0-9")
			},
		},
		{
			s: "1-10",
			rg: Range{
				Start:  1,
				Length: 10,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "1-10")
			},
		},
		{
			s: "1-0",
			rg: Range{
				Start:  1,
				Length: 0,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "1-0")
			},
		},
		{
			s: "1-1",
			rg: Range{
				Start:  1,
				Length: 1,
			},
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "1-1")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			tc.expect(t, tc.rg.URLMetaString())
		})
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		s    string
		size int64
		rg   []Range
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

	for _, tc := range tests {
		rg := tc.rg
		ranges, err := ParseRange(tc.s, tc.size)
		if err != nil && rg != nil {
			t.Errorf("ParseRange(%q) returned error %q", tc.s, err)
		}

		if len(ranges) != len(rg) {
			t.Errorf("len(ParseRange(%q)) = %d, want %d", tc.s, len(ranges), len(rg))
			continue
		}

		for i := range rg {
			if ranges[i].Start != rg[i].Start {
				t.Errorf("ParseRange(%q)[%d].Serve = %d, want %d", tc.s, i, ranges[i].Start, rg[i].Start)
			}

			if ranges[i].Length != rg[i].Length {
				t.Errorf("ParseRange(%q)[%d].Length = %d, want %d", tc.s, i, ranges[i].Length, rg[i].Length)
			}
		}
	}
}

func TestParseOneRange(t *testing.T) {
	tests := []struct {
		s    string
		size int64
		rg   Range
	}{
		{"bytes=0-9", 10, Range{0, 10}},
		{"bytes=0-", 10, Range{0, 10}},
		{"bytes=5-", 10, Range{5, 5}},
		{"bytes=0-20", 10, Range{0, 10}},
		{"bytes=1-2", 10, Range{1, 2}},
		{"bytes=0-0", 11, Range{0, 1}},
		{"bytes=-5", 10, Range{5, 5}},
		{"bytes=-15", 10, Range{0, 10}},
		{"bytes=0-499", 10000, Range{0, 500}},
		{"bytes=500-999", 10000, Range{500, 500}},
		{"bytes=-500", 10000, Range{9500, 500}},
		{"bytes=9500-", 10000, Range{9500, 500}},
		{"bytes=0-0", 10000, Range{0, 1}},
		{"bytes=500-600", 10000, Range{500, 101}},
		{"bytes=500-700", 10000, Range{500, 201}},

		// Match Apache laxity:
		{"bytes=   1 -2     ", 11, Range{1, 2}},
	}

	for _, tc := range tests {
		erg := tc.rg
		rg, err := ParseOneRange(tc.s, tc.size)
		if err != nil {
			t.Errorf("ParseOneRange(%q) returned error %q", tc.s, err)
		}

		if rg.Start != erg.Start {
			t.Errorf("ParseOneRange(%q).Serve = %d, want %d", tc.s, rg.Start, erg.Start)
		}

		if rg.Length != erg.Length {
			t.Errorf("ParseOneRange(%q).Length = %d, want %d", tc.s, rg.Length, erg.Length)
		}
	}
}

func TestParseURLMetaRange(t *testing.T) {
	tests := []struct {
		s      string
		size   int64
		expect func(t *testing.T, rg Range, err error)
	}{
		{
			s:    "0-65575",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(rg, Range{
					Start:  0,
					Length: 65576,
				})
			},
		},
		{
			s:    "2-2",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(rg, Range{
					Start:  2,
					Length: 1,
				})
			},
		},
		{
			s:    "2-",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(rg, Range{
					Start:  2,
					Length: 65574,
				})
			},
		},
		{
			s:    "-100",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(rg, Range{
					Start:  65476,
					Length: 100,
				})
			},
		},
		{
			s:    "0-66575",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(rg, Range{
					Start:  0,
					Length: 65576,
				})
			},
		},
		{
			s:    "0-65-575",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			s:    "0-hello",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			s:    "65575-0",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			s:    "-1-8",
			size: 65576,
			expect: func(t *testing.T, rg Range, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			rg, err := ParseURLMetaRange(tc.s, tc.size)
			tc.expect(t, rg, err)
		})
	}
}

func TestMustParseRang(t *testing.T) {
	tests := []struct {
		s     string
		size  int64
		rg    Range
		panic bool
	}{
		{"bytes=0-9", 10, Range{0, 10}, false},
		{"-10", 10, Range{}, true},
		{"bytes=500-700,601-999", 10000, Range{}, true},
	}

	for _, tc := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil && !tc.panic {
					t.Errorf("Unexpected panic: %v", r)
				} else if r == nil && tc.panic {
					t.Errorf("Expected panic but did not panic")
				}
			}()

			erg := tc.rg
			rg := MustParseRange(tc.s, tc.size)

			if rg.Start != erg.Start {
				t.Errorf("MustParseRange(%q).Serve = %d, want %d", tc.s, rg.Start, erg.Start)
			}

			if rg.Length != erg.Length {
				t.Errorf("MustParseRange(%q).Length = %d, want %d", tc.s, rg.Length, erg.Length)
			}
		}()
	}
}
