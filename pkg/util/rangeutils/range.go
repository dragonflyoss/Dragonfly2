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
	"fmt"
	"strconv"
	"strings"
)

const (
	separator = "-"
)

type Range struct {
	StartIndex uint64
	EndIndex   uint64
}

func (r Range) String() string {
	return fmt.Sprintf("%d%s%d", r.StartIndex, separator, r.EndIndex)
}

// ParseRange parses Range according to range string.
// rangeStr: "start-end"
func ParseRange(rangeStr string) (r *Range, err error) {
	ranges := strings.Split(rangeStr, separator)
	if len(ranges) != 2 {
		return nil, fmt.Errorf("range value(%s) is illegal which should be like 0-45535", rangeStr)
	}

	startIndex, err := strconv.ParseUint(ranges[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range(%s) start is not a non-negative number", rangeStr)
	}
	endIndex, err := strconv.ParseUint(ranges[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range(%s) end is not a non-negative number", rangeStr)
	}

	if endIndex < startIndex {
		return nil, fmt.Errorf("range(%s) start is larger than end", rangeStr)
	}

	return &Range{
		StartIndex: startIndex,
		EndIndex:   endIndex,
	}, nil
}

func GetBreakRange(breakPoint int64, rangeLength int64) (*Range, error) {
	if breakPoint < 0 {
		return nil, fmt.Errorf("breakPoint is illegal for value: %d", breakPoint)
	}
	if rangeLength < 0 {
		return nil, fmt.Errorf("rangeLength is illegal for value: %d", rangeLength)
	}
	end := rangeLength - 1
	if breakPoint > end {
		return nil, fmt.Errorf("start: %d is larger than end: %d", breakPoint, end)

	}
	return &Range{
		StartIndex: uint64(breakPoint),
		EndIndex:   uint64(end),
	}, nil
}
