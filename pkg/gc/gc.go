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
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
)

type GC interface {
	// Add adds GC task
	Add(interval time.Duration, name string, t Task)

	// AddTask adds GC task
	ForceRun(name string)

	// AddTask adds GC task
	ForceRunAll()

	// Serve running the GC task
	Serve() error

	// Stop running the GC task
	Stop()
}

type gc struct {
	tasks sync.Map
	done  chan bool
}

func AddTask(interval time.Duration, name string, t Task) {
	tasks[name] = gc
}

func New(interval time.Duration) GC {
	return &gc{
		tasks: &sync.Map{},
		done:  make(chan bool),
	}
}

func (g gcManager) Ser() {
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
