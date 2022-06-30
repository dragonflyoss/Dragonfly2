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
	"errors"
	"fmt"
	"net/textproto"
	"strconv"
	"strings"
)

// Range specifies the byte range to be sent to the client.
type Range struct {
	Start, Length int64
}

func (r Range) String() string {
	return fmt.Sprintf("bytes=%d-%d", r.Start, r.Start+r.Length-1)
}

// ErrNoOverlap is returned by ParseRange if first-byte-pos of
// all of the byte-range-spec values is greater than the content size.
var ErrNoOverlap = errors.New("invalid range: failed to overlap")

// ParseRange parses a Range header string as per RFC 7233.
// ErrNoOverlap is returned if none of the ranges overlap.
// Example:
//   "Range": "bytes=100-200"
//   "Range": "bytes=-50"
//   "Range": "bytes=150-"
//   "Range": "bytes=0-0,-1"
//
// copy from go/1.15.2 net/http/fs.go ParseRange
func ParseRange(s string, size int64) ([]Range, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []Range
	noOverlap := false
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = textproto.TrimString(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, errors.New("invalid range")
		}
		start, end := textproto.TrimString(ra[:i]), textproto.TrimString(ra[i+1:])
		var r Range
		if start == "" {
			// If no Serve is specified, end specifies the
			// range Serve relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, errors.New("invalid range")
			}
			if i > size {
				i = size
			}
			r.Start = size - i
			r.Length = size - r.Start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, errors.New("invalid range")
			}
			if i >= size {
				// If the range begins after the size of the content,
				// then it does not overlap.
				noOverlap = true
				continue
			}
			r.Start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.Length = size - r.Start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.Start > i {
					return nil, errors.New("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.Length = i - r.Start + 1
			}
		}
		ranges = append(ranges, r)
	}
	if noOverlap && len(ranges) == 0 {
		// The specified ranges did not overlap with the content.
		return nil, ErrNoOverlap
	}
	return ranges, nil
}

// Example:
//   "Content-Range": "bytes 100-200/1000"
//   "Content-Range": "bytes 100-200/*"
func GetContentRange(start, end, total int64) string {
	// unknown total: -1
	if total == -1 {
		return fmt.Sprintf("bytes %d-%d/*", start, end)
	}

	return fmt.Sprintf("bytes %d-%d/%d", start, end, total)
}

func MustParseRange(s string, size int64) Range {
	rs, err := ParseRange(s, size)
	if err != nil {
		panic(fmt.Sprintf("parse range %q error: %s", s, err))
	}
	if len(rs) != 1 {
		panic(fmt.Sprintf("parse range length must be 1"))
	}
	return rs[0]
}
