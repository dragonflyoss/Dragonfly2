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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"gopkg.in/yaml.v3"

	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

type Bytes int64

const (
	B  Bytes = 1
	KB       = 1024 * B
	MB       = 1024 * KB
	GB       = 1024 * MB
	TB       = 1024 * GB
	PB       = 1024 * TB
	EB       = 1024 * PB
)

func (f Bytes) ToNumber() int64 {
	return int64(f)
}

func ToBytes(size int64) Bytes {
	return Bytes(size)
}

// Set is used for command flag var
func (f *Bytes) Set(s string) (err error) {
	if pkgstrings.IsBlank(s) {
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

var sizeRegexp = regexp.MustCompile(`^([0-9]+)(\.0*)?([MmKkGgTtPpEe])?[iI]?[bB]?$`)

func parseSize(fsize string) (Bytes, error) {
	if pkgstrings.IsBlank(fsize) {
		return 0, nil
	}

	matches := sizeRegexp.FindStringSubmatch(fsize)
	if len(matches) == 0 {
		return 0, fmt.Errorf("parse size %s: invalid format", fsize)
	}

	var unit Bytes
	switch matches[3] {
	case "k", "K":
		unit = KB
	case "m", "M":
		unit = MB
	case "g", "G":
		unit = GB
	case "t", "T":
		unit = TB
	case "p", "P":
		unit = PB
	case "e", "E":
		unit = EB
	default:
		unit = B
	}

	num, err := strconv.ParseInt(matches[1], 0, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size %s: %w", fsize, err)
	}

	return ToBytes(num) * unit, nil
}

func (f Bytes) MarshalYAML() (any, error) {
	result := f.String()
	return result, nil
}

func (f *Bytes) UnmarshalJSON(b []byte) error {
	return f.unmarshal(json.Unmarshal, b)
}

func (f *Bytes) UnmarshalYAML(node *yaml.Node) error {
	return f.unmarshal(yaml.Unmarshal, []byte(node.Value))
}

func (f *Bytes) unmarshal(unmarshal func(in []byte, out any) (err error), b []byte) error {
	var v any
	if err := unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*f = Bytes(int64(value))
		return nil
	case int:
		*f = Bytes(int64(value))
		return nil
	case int64:
		*f = Bytes(value)
		return nil
	case string:
		size, err := parseSize(value)
		if err != nil {
			return fmt.Errorf("invalid byte size: %w", err)
		}
		*f = size
		return nil
	default:
		return errors.New("invalid byte size")
	}
}
