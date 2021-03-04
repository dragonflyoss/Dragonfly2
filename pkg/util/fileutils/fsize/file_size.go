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

package fsize

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

type Size int64

const (
	B  Size = 1
	KB      = 1024 * B
	MB      = 1024 * KB
	GB      = 1024 * MB
)

func (f Size) ToNumber() int64 {
	return int64(f)
}

func ToFsize(size int64) Size {
	return Size(size)
}

func (f *Size) Set(s string) (err error) {
	if stringutils.IsBlank(s) {
		*f = 0
	} else {
		*f, err = parseSize(s)
	}

	return
}

func (f Size) Type() string {
	return "FileSize"
}

func (f Size) String() string {
	var (
		symbol string
		unit   Size
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

var sizeRegexp = regexp.MustCompile(`^([0-9]+)(\.0*)?(MB?|m|KB?|k|GB?|g|B)?$`)

func parseSize(fsize string) (Size, error) {
	fsize = strings.TrimSpace(fsize)
	if stringutils.IsBlank(fsize) {
		return 0, nil
	}

	matches := sizeRegexp.FindStringSubmatch(fsize)
	if len(matches) == 0 {
		return 0, errors.Errorf("parse size %s: invalid format", fsize)
	}

	var unit Size
	switch matches[3] {
	case "k", "K", "KB":
		unit = KB
	case "m", "M", "MB":
		unit = MB
	case "g", "G", "GB":
		unit = GB
	default:
		unit = B
	}

	num, err := strconv.ParseInt(matches[1], 0, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse size:%s", fsize)
	}

	return ToFsize(num) * unit, nil
}
