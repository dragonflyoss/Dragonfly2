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

package rangeutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRange(t *testing.T) {
	var cases = []struct {
		rangeStr string
		length   uint64
		expected *Range
		wantErr  bool
	}{
		{
			rangeStr: "0-65575",
			length:   65576,
			expected: &Range{
				StartIndex: 0,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr: "2-2",
			length:   65576,
			expected: &Range{
				StartIndex: 2,
				EndIndex:   2,
			},
			wantErr: false,
		}, {
			rangeStr: "2-",
			length:   65576,
			expected: &Range{
				StartIndex: 2,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr: "-100",
			length:   65576,
			expected: &Range{
				StartIndex: 65476,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr: "0-66575",
			length:   65576,
			expected: &Range{
				StartIndex: 0,
				EndIndex:   65575,
			},
			wantErr: false,
		},
		{
			rangeStr: "0-65-575",
			length:   65576,
			expected: nil,
			wantErr:  true,
		},
		{
			rangeStr: "0-hello",
			length:   65576,
			expected: nil,
			wantErr:  true,
		},
		{
			rangeStr: "65575-0",
			length:   65576,
			expected: nil,
			wantErr:  true,
		},
		{
			rangeStr: "-1-8",
			length:   65576,
			expected: nil,
			wantErr:  true,
		},
	}

	for _, v := range cases {
		result, err := ParseRange(v.rangeStr, v.length)
		assert.Equal(t, v.expected, result)
		assert.Equal(t, v.wantErr, err != nil)
	}
}
