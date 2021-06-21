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

package gc

import (
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type GC interface {
	TryGC() (bool, error)
}

type Manager interface {
	Start()
	Stop()
}

type gcManager struct {
	interval time.Duration
	done     chan bool
}

var _ Manager = (*gcManager)(nil)

var allGCTasks = map[string]GC{}

func Register(name string, gc GC) {
	allGCTasks[name] = gc
}

func NewManager(interval time.Duration) Manager {
	return &gcManager{
		interval: interval,
		done:     make(chan bool),
	}
}

func (g gcManager) Start() {
	go func() {
		tick := time.NewTicker(g.interval)
		for {
			select {
			case <-tick.C:
				for name, gc := range allGCTasks {
					var log = logger.With("component", name)
					log.Debugf("start gc")
					_, err := gc.TryGC()
					if err != nil {
						log.Errorf("gc %s error: %s", name, err)
					}
					log.Debugf("gc done")
				}
			case <-g.done:
				logger.Infof("gc exited")
				return
			}
		}
	}()
}

func (g gcManager) Stop() {
	close(g.done)
}
