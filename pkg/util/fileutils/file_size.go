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

package fileutils

import (
	"fmt"
	"regexp"
	"strconv"

	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

type Fsize int64

var sizeRegexp = regexp.MustCompile("^([0-9]+)(MB?|m|KB?|k|GB?|g|B)$")

func (f *Fsize) Set(s string) (err error) {
	if stringutils.IsBlank(s) {
		*f = ToFsize(0)
	} else {
		*f, err = ParseSize(s)
	}

	return
}

func (f Fsize) Type() string {
	return "FileSize"
}

const (
	B  Fsize = 1
	KB       = 1024 * B
	MB       = 1024 * KB
	GB       = 1024 * MB
)

func (f Fsize) ToNumber() int64 {
	return int64(f)
}

func ToFsize(size int64) Fsize {
	return Fsize(size)
}

func (f Fsize) String() string {
	var (
		symbol string
		unit   Fsize
	)

	if f >= GB {
		symbol = "GB"
		unit = GB
	} else if f >= MB {
		symbol = "MB"
		unit = MB
	} else if f >= KB {
		symbol = "KB"
		unit = KB
	} else {
		symbol = "B"
		unit = B
	}

	return fmt.Sprintf("%.1f%s", float64(f)/float64(unit), symbol)
}

// ParseSize parses a string into a int64.
func ParseSize(fsize string) (Fsize, error) {
	var n int
	n, err := strconv.Atoi(fsize)
	if err == nil && n >= 0 {
		return Fsize(n), nil
	}

	if n < 0 {
		return 0, errors.Wrapf(dferrors.ErrInvalidArgument, "not a valid fsize string: %d, only non-negative values are supported", fsize)
	}

	matches := sizeRegexp.FindStringSubmatch(fsize)
	if len(matches) != 3 {
		return 0, errors.Wrapf(dferrors.ErrInvalidArgument, "%s and supported format: G(B)/M(B)/K(B)/B or pure number", fsize)
	}
	n, _ = strconv.Atoi(matches[1])
	switch unit := matches[2]; {
	case unit == "g" || unit == "G" || unit == "GB":
		n *= int(GB)
	case unit == "m" || unit == "M" || unit == "MB":
		n *= int(MB)
	case unit == "k" || unit == "K" || unit == "KB":
		n *= int(KB)
	case unit == "B":
		// Value already correct
	default:
		return 0, errors.Wrapf(dferrors.ErrInvalidArgument, "%s and supported format: G(B)/M(B)/K(B)/B or pure number", fsize)
	}
	return Fsize(n), nil
}
