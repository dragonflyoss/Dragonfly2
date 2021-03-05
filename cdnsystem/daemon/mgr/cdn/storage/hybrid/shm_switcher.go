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
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"go.uber.org/atomic"
)

type shmSwitcher struct {
	off             *atomic.Bool
	whiteList       []string
	useShmThreshold *atomic.Int64
}

func newShmSwitcherService() *shmSwitcher {
	return &shmSwitcher{
		off:             atomic.NewBool(false),
		whiteList:       nil,
		useShmThreshold: atomic.NewInt64(1024 * 1024 * 1024),
	}
}

func (s *shmSwitcher) updateWhiteList(newWhiteList []string) {

}

func (s *shmSwitcher) updateSwitcher(switcher string) {
	//if s.off.Load() ^ strings.EqualFold("off", strings.ToLower(switcher)) {
	//	s.off.Store() = !s.off
	//}
	logger.Infof("shm off-switcher changed to {}", s.off)
}

func (s *shmSwitcher) updateThreshold(threshold int) {
	//if threshold == nil || s.useShmThreshold == (threshold * 1024 * 1024) {
	//return;
	//}
	//s.useShmThreshold = threshold * 1024 * 1024
	logger.Infof("shm threshold changed to %d MB", threshold)
}

func (s *shmSwitcher) check(url string, fileLength int64) bool {
	//if !s.off.Load() {
	//	if fileLength == 0 || fileLength < s.useShmThreshold.Load() {
	//		return false
	//	}
	//	if len(s.whiteList) == 0 {
	//		return true
	//	}
	//	for _, reg := range s.whiteList {
	//		if matched, err := regexp.MatchString(reg, url); err == nil && matched {
	//			return true
	//		}
	//	}
	//}
	return true
}