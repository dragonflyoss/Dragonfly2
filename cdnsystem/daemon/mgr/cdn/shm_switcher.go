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

package cdn

import (
	"go.uber.org/atomic"
	"regexp"
)

type shmSwitcherService struct {
	off             *atomic.Bool
	whiteList       []string
	useShmThreshold *atomic.Int64
}

func newShmSwitcherService() *shmSwitcherService {
	return &shmSwitcherService{
		off:             &atomic.Bool{},
		whiteList:       nil,
		useShmThreshold: atomic.NewInt64(1024 * 1024 * 1024),
	}
}

func (sss *shmSwitcherService) check(url string, fileLength int64) bool {
	if !sss.off.Load() {
		if fileLength == 0 || fileLength < sss.useShmThreshold.Load() {
			return false
		}
		if len(sss.whiteList) == 0 {
			return true
		}
		for _, reg := range sss.whiteList {
			if matched, err := regexp.MatchString(reg, url); err == nil && matched {
				return true
			}
		}
	}
	return false
}
