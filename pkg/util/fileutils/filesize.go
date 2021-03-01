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
)

type Fsize int64

const (
	B  Fsize = 1
	KB       = 1024 * B
	MB       = 1024 * KB
	GB       = 1024 * MB
)

func (f Fsize) Number() int64 {
	return int64(f)
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
