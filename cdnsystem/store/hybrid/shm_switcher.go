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

import "sync"

type ShmSwitcherService struct {
	switcher        bool
	whiteList       []string
	useShmThreshold int64
	sync.Mutex
}

func NewShmSwitcher() *ShmSwitcherService {
	return &ShmSwitcherService{
		switcher:        false,
		whiteList:       nil,
		useShmThreshold: 1024 * 1024 * 1024,
		Mutex:           sync.Mutex{},
	}
}

func UpdateSwitcher(shmSwitcher string) {

}

// check Check if SHM can be used
func check(url string, fileLength int64) bool {
	return false
}
