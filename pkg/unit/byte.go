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

package unit

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

type Bytes int64

const (
	B  Bytes = 1
	KB       = 1024 * B
	MB       = 1024 * KB
	GB       = 1024 * MB
	TB       = 1024 * GB
	PB       = 1024 * TB
)

func (f Bytes) ToNumber() int64 {
	return int64(f)
}

func ToBytes(size int64) Bytes {
	return Bytes(size)
}

// Set is used for command flag var
func (f *Bytes) Set(s string) (err error) {
	if stringutils.IsBlank(s) {
		*f = 0
	} else {
		*f, err = parseSize(s)
	}

	return
}

func (f Bytes) Type() string {
	return "bytes"
}

func (f Bytes) String() string {
	var (
		symbol string
		unit   Bytes
	)

	if f >= PB {
		symbol = "PB"
		unit = PB
	} else if f >= TB {
		symbol = "TB"
		unit = TB
	} else if f >= GB {
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

func parseSize(fsize string) (Bytes, error) {
	fsize = strings.TrimSpace(fsize)
	if stringutils.IsBlank(fsize) {
		return 0, nil
	}

	matches := sizeRegexp.FindStringSubmatch(fsize)
	if len(matches) == 0 {
		return 0, errors.Errorf("parse size %s: invalid format", fsize)
	}

	var unit Bytes
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

	return ToBytes(num) * unit, nil
}

func (f Bytes) MarshalYAML() (interface{}, error) {
	result := f.String()
	return result, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (f *Bytes) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var fsizeStr string
	if err := unmarshal(&fsizeStr); err != nil {
		return err
	}

	fsize, err := parseSize(fsizeStr)
	if err != nil {
		return err
	}
	*f = fsize
	return nil
}
