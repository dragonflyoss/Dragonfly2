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
	"context"
	"strings"
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type Executor interface {
	GC() error
}

type ExecutorWrapper struct {
	gcInitialDelay time.Duration
	gcInterval     time.Duration
	gcExecutor     Executor
}

var (
	gcExecutorWrappers = make(map[string]*ExecutorWrapper)
)

// Register a gc task
func Register(name string, gcInitialDelay time.Duration, gcInterval time.Duration, gcExecutor Executor) {
	gcExecutorWrappers[strings.ToLower(name)] = &ExecutorWrapper{
		gcInitialDelay: gcInitialDelay,
		gcInterval:     gcInterval,
		gcExecutor:     gcExecutor,
	}
}

// StartGC starts to do the gc jobs.
func StartGC(ctx context.Context) error {
	logger.Debugf("====start the gc jobs====")
	var wg sync.WaitGroup
	for name, executorWrapper := range gcExecutorWrappers {
		wg.Add(1)
		// start a goroutine to gc
		go func(name string, wrapper *ExecutorWrapper) {
			logger.Debugf("start the %s gc task", name)
			// delay to execute GC after initialDelay
			time.Sleep(wrapper.gcInitialDelay)
			wg.Done()
			// execute the GC by fixed delay
			ticker := time.NewTicker(wrapper.gcInterval)
			for {
				select {
				case <-ctx.Done():
					logger.Infof("exit %s gc task", name)
					return
				case <-ticker.C:
					if err := wrapper.gcExecutor.GC(); err != nil {
						logger.Errorf("%s gc task execute failed: %v", name, err)
					}
				}
			}
		}(name, executorWrapper)
	}
	wg.Wait()
	logger.Debugf("====all gc jobs have been launched====")
	return nil
}
