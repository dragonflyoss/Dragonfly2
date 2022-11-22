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

//go:generate mockgen -destination gc_mock.go -source gc.go -package gc

package gc

import (
	"fmt"
	"sync"
	"time"
)

// GC is the interface used for release resource.
type GC interface {
	// Add adds GC task.
	Add(Task) error

	// Run GC task.
	Run(string) error

	// Run all registered GC tasks.
	RunAll()

	// Start running the GC task.
	Start()

	// Stop running the GC task.
	Stop()
}

// GC provides task release function.
type gc struct {
	tasks  *sync.Map
	logger Logger
	done   chan struct{}
}

// Option is a functional option for configuring the GC.
type Option func(g *gc)

// WithLogger set the logger for GC.
func WithLogger(logger Logger) Option {
	return func(g *gc) {
		g.logger = logger
	}
}

// New returns a new GC instence.
func New(options ...Option) GC {
	g := &gc{
		tasks:  &sync.Map{},
		logger: &gcLogger{},
		done:   make(chan struct{}),
	}

	for _, opt := range options {
		opt(g)
	}

	return g
}

func (g gc) Add(t Task) error {
	if err := t.validate(); err != nil {
		return err
	}

	g.tasks.Store(t.ID, t)
	return nil
}

func (g gc) Run(id string) error {
	v, ok := g.tasks.Load(id)
	if !ok {
		return fmt.Errorf("can not find task %s", id)
	}

	go g.run(v.(Task))
	return nil
}

func (g gc) RunAll() {
	g.runAll()
}

func (g gc) Start() {
	g.tasks.Range(func(k, v any) bool {
		go func() {
			task := v.(Task)
			tick := time.NewTicker(task.Interval)
			for {
				select {
				case <-tick.C:
					g.run(task)
				case <-g.done:
					g.logger.Infof("%s GC stop", k)
					return
				}
			}
		}()
		return true
	})
}

func (g gc) Stop() {
	close(g.done)
}

func (g gc) runAll() {
	g.tasks.Range(func(k, v any) bool {
		go g.run(v.(Task))
		return true
	})
}

func (g gc) run(t Task) {
	done := make(chan struct{})

	go func() {
		g.logger.Infof("%s GC start", t.ID)
		defer close(done)

		if err := t.Runner.RunGC(); err != nil {
			g.logger.Errorf("%s GC error: %v", t.ID, err)
			return
		}
	}()

	select {
	case <-time.After(t.Timeout):
		g.logger.Infof("%s GC timeout", t.ID)
	case <-done:
		g.logger.Infof("%s GC done", t.ID)
	}
}
