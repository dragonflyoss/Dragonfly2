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

func TestParseRange(t *testing.T) {
	var cases = []struct {
		rangeStr     string
		totalLength  uint64
		targetLength uint64
		expected     *Range
		wantErr      bool
	}{
		{
			rangeStr:     "0-65575",
			totalLength:  65576,
			targetLength: 65576,
			expected: &Range{
				StartIndex: 0,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr:     "2-2",
			totalLength:  65576,
			targetLength: 1,
			expected: &Range{
				StartIndex: 2,
				EndIndex:   2,
			},
			wantErr: false,
		}, {
			rangeStr:     "2-",
			totalLength:  65576,
			targetLength: 65574,
			expected: &Range{
				StartIndex: 2,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr:     "-100",
			totalLength:  65576,
			targetLength: 100,
			expected: &Range{
				StartIndex: 65476,
				EndIndex:   65575,
			},
			wantErr: false,
		}, {
			rangeStr:     "0-66575",
			totalLength:  65576,
			targetLength: 65576,
			expected: &Range{
				StartIndex: 0,
				EndIndex:   65575,
			},
			wantErr: false,
		},
		{
			rangeStr:    "0-65-575",
			totalLength: 65576,
			expected:    nil,
			wantErr:     true,
		},
		{
			rangeStr:    "0-hello",
			totalLength: 65576,
			expected:    nil,
			wantErr:     true,
		},
		{
			rangeStr:    "65575-0",
			totalLength: 65576,
			expected:    nil,
			wantErr:     true,
		},
		{
			rangeStr:    "-1-8",
			totalLength: 65576,
			expected:    nil,
			wantErr:     true,
		},
	}

	for _, v := range cases {
		t.Run(v.rangeStr, func(t *testing.T) {
			result, err := ParseRange(v.rangeStr, v.totalLength)
			assert.Equal(t, v.expected, result)
			assert.Equal(t, v.wantErr, err != nil)
			if !v.wantErr {
				assert.Equal(t, v.targetLength, result.Length())
			}
		})
	}
}
