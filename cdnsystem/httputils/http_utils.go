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

package httputils

import (
	"fmt"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"github.com/pkg/errors"
)

// ConstructRangeStr wraps the rangeStr as a HTTP Range header value.
func ConstructRangeStr(rangeStr string) string {
	return fmt.Sprintf("bytes=%s", rangeStr)
}

var validURLSchemas = "https?|HTTPS?"

func GetValidURLSchemas() string {
	return validURLSchemas
}

// RangeStruct contains the start and end of a http header range.
type RangeStruct struct {
	StartIndex int64
	EndIndex   int64
}

// GetRangeSE parses the start and the end from range HTTP header and returns them.
func GetRangeSE(rangeHTTPHeader string, length int64) ([]*RangeStruct, error) {
	var rangeStr = rangeHTTPHeader

	// when rangeHTTPHeader looks like "bytes=0-1023", and then gets "0-1023".
	if strings.ContainsAny(rangeHTTPHeader, "=") {
		rangeSlice := strings.Split(rangeHTTPHeader, "=")
		if len(rangeSlice) != 2 {
			return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid range: %s, should be like bytes=0-1023", rangeStr)
		}
		rangeStr = rangeSlice[1]
	}

	var result []*RangeStruct

	rangeArr := strings.Split(rangeStr, ",")
	rangeCount := len(rangeArr)
	if rangeCount == 0 {
		result = append(result, &RangeStruct{
			StartIndex: 0,
			EndIndex:   length - 1,
		})
		return result, nil
	}

	for i := 0; i < rangeCount; i++ {
		if strings.Count(rangeArr[i], "-") != 1 {
			return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid range: %s, should be like 0-1023", rangeArr[i])
		}

		// -{length}
		if strings.HasPrefix(rangeArr[i], "-") {
			rangeStruct, err := handlePrefixRange(rangeArr[i], length)
			if err != nil {
				return nil, err
			}
			result = append(result, rangeStruct)
			continue
		}

		// {startIndex}-
		if strings.HasSuffix(rangeArr[i], "-") {
			rangeStruct, err := handleSuffixRange(rangeArr[i], length)
			if err != nil {
				return nil, err
			}
			result = append(result, rangeStruct)
			continue
		}

		rangeStruct, err := handlePairRange(rangeArr[i], length)
		if err != nil {
			return nil, err
		}
		result = append(result, rangeStruct)
	}
	return result, nil
}

func handlePrefixRange(rangeStr string, length int64) (*RangeStruct, error) {
	downLength, err := strconv.ParseInt(strings.TrimPrefix(rangeStr, "-"), 10, 64)
	if err != nil || downLength < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}

	if downLength > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	return &RangeStruct{
		StartIndex: length - downLength,
		EndIndex:   length - 1,
	}, nil
}

func handleSuffixRange(rangeStr string, length int64) (*RangeStruct, error) {
	startIndex, err := strconv.ParseInt(strings.TrimSuffix(rangeStr, "-"), 10, 64)
	if err != nil || startIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}

	if startIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	return &RangeStruct{
		StartIndex: startIndex,
		EndIndex:   length - 1,
	}, nil
}

func handlePairRange(rangeStr string, length int64) (*RangeStruct, error) {
	rangePair := strings.Split(rangeStr, "-")

	startIndex, err := strconv.ParseInt(rangePair[0], 10, 64)
	if err != nil || startIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}
	if startIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	endIndex, err := strconv.ParseInt(rangePair[1], 10, 64)
	if err != nil || endIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}
	if endIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	if endIndex < startIndex {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "range: %s, the start is larger the end", rangeStr)
	}

	return &RangeStruct{
		StartIndex: startIndex,
		EndIndex:   endIndex,
	}, nil
}
