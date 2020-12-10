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

package daemon

import (
	"time"

	"github.com/sirupsen/logrus"
)

type GC interface {
	TryGC() (bool, error)
}

type GCManager interface {
	Start()
	Stop()
}

type gcManager struct {
	interval time.Duration
}

var allGCTasks = map[string]GC{}

func RegisterGC(name string, gc GC, interval time.Duration) {
	allGCTasks[name] = gc
}

func NewGCManager(interval time.Duration) GCManager {
	return &gcManager{
		interval: interval,
	}
}

func (g gcManager) Start() {
	go func() {
		tick := time.Tick(g.interval)
		for {
			select {
			case <-tick:
				for name, gc := range allGCTasks {
					logrus.Infof("start gc %s", name)
					_, err := gc.TryGC()
					if err != nil {
						logrus.Errorf("gc %s error: %s", name, err)
					}
				}
			}
		}
	}()
}

func (g gcManager) Stop() {
	panic("implement me")
}
