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

package hybrid

import (
	"regexp"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type shmSwitch struct {
	off             *atomic.Bool
	whiteList       []string
	useShmThreshold *atomic.Int64
}

func newShmSwitch() *shmSwitch {
	return &shmSwitch{
		off:             atomic.NewBool(false),
		whiteList:       nil,
		useShmThreshold: atomic.NewInt64(1024 * 1024 * 1024),
	}
}

func (s *shmSwitch) updateWhiteList(newWhiteList []string) {
	logger.Infof("shm whiteList changed to {}", s.whiteList)
}

func (s *shmSwitch) updateSwitcher(switcher string) {
	logger.Infof("shm off-switcher changed to {}", s.off)
}

func (s *shmSwitch) updateThreshold(threshold int) {
	logger.Infof("shm threshold changed to %d MB", s.useShmThreshold)
}

func (s *shmSwitch) check(url string, fileLength int64) bool {
	if !s.off.Load() {
		if fileLength == 0 || fileLength < s.useShmThreshold.Load() {
			return false
		}
		if len(s.whiteList) == 0 {
			return true
		}
		for _, reg := range s.whiteList {
			if matched, err := regexp.MatchString(reg, url); err == nil && matched {
				return true
			}
		}
	}
	return true
}
