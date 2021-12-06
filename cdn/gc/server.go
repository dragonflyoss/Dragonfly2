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
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type Server struct {
	config Config
	done   chan struct{}
	wg     *sync.WaitGroup
}

func New(config Config) (*Server, error) {
	config = config.applyDefaults()
	// scheduler config values
	s, err := yaml.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal gc server config")
	}
	logger.Infof("gc server config: \n%s", s)
	return &Server{
		config: config,
		done:   make(chan struct{}),
		wg:     new(sync.WaitGroup),
	}, nil
}

func (server *Server) Serve() error {
	logger.Info("====starting gc jobs====")
	for name, executorWrapper := range gcExecutorWrappers {
		server.wg.Add(1)
		// start a goroutine to gc
		go func(name string, wrapper *ExecutorWrapper) {
			defer server.wg.Done()
			logger.Debugf("start the %s gc task， gc initialDelay: %s, gc initial interval: %s", name, wrapper.gcInitialDelay, wrapper.gcInterval)
			// delay executing GC after initialDelay
			time.Sleep(wrapper.gcInitialDelay)
			// execute the GC by fixed delay
			ticker := time.NewTicker(wrapper.gcInterval)
			for {
				select {
				case <-server.done:
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
	server.wg.Wait()
	return nil
}

func (server *Server) Shutdown() error {
	defer logger.Infof("====stopped gc server====")
	server.done <- struct{}{}
	server.wg.Wait()
	return nil
}

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
