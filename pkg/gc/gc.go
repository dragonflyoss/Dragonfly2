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
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// GC is the interface used for release resource
type GC interface {
	// Add adds GC task
	Add(string, Task)

	// Run GC task
	Run(string) error

	// Run all registered GC tasks
	RunAll()

	// Serve running the GC task
	Serve()

	// Stop running the GC task
	Stop()
}

// GC provides task release function
type gc struct {
	tasks    *sync.Map
	interval time.Duration
	timeout  time.Duration
	logger   Logger
	done     chan bool
}

// Option is a functional option for configuring the GC
type Option func(g *gc)

// WithInterval set the interval for GC collection
func WithInterval(interval time.Duration) Option {
	return func(g *gc) {
		g.interval = interval
	}
}

// WithTimeout set the timeout for GC collection
func WithTimeout(timeout time.Duration) Option {
	return func(g *gc) {
		g.timeout = timeout
	}
}

// WithLogger set the logger for GC
func WithLogger(logger Logger) Option {
	return func(g *gc) {
		g.logger = logger
	}
}

// New returns a new GC instence
func New(options ...Option) (GC, error) {
	g := &gc{
		tasks:  &sync.Map{},
		done:   make(chan bool),
		logger: logrus.New(),
	}

	for _, opt := range options {
		opt(g)
	}

	if err := g.validate(); err != nil {
		return nil, err
	}

	return g, nil
}

func (g gc) Add(k string, t Task) {
	g.tasks.Store(k, t)
}

func (g gc) Run(k string) error {
	v, ok := g.tasks.Load(k)
	if !ok {
		return errors.New("can not find the task")
	}

	go g.run(context.Background(), k, v.(Task))
	return nil
}

func (g gc) RunAll() {
	g.runAll(context.Background())
}

func (g gc) Serve() {
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tick := time.NewTicker(g.interval)
		for {
			select {
			case <-tick.C:
				g.runAll(ctx)
			case <-g.done:
				g.logger.Infof("GC stop")
				return
			}
		}
	}()
}

func (g gc) Stop() {
	close(g.done)
}

func (g gc) validate() error {
	if g.interval <= 0 {
		return errors.New("interval value is greater than 0")
	}

	if g.timeout <= 0 {
		return errors.New("timeout value is greater than 0")
	}

	if g.timeout >= g.interval {
		return errors.New("timeout value needs to be less than the interval value")
	}

	return nil
}

func (g gc) runAll(ctx context.Context) {
	g.tasks.Range(func(k, v interface{}) bool {
		go g.run(ctx, k.(string), v.(Task))
		return true
	})
}

func (g gc) run(ctx context.Context, k string, t Task) {
	done := make(chan struct{})

	go func() {
		g.logger.Infof("%s GC %s", k, "start")
		defer close(done)
		if err := t.RunGC(); err != nil {
			g.logger.Errorf("%s GC error: %v", k, err)
			return
		}
	}()

	select {
	case <-time.After(g.timeout):
		g.logger.Infof("%s GC %s", k, "timeout")
	case <-done:
		g.logger.Infof("%s GC %s", k, "done")
	case <-ctx.Done():
		g.logger.Infof("%s GC %s", k, "stop")
	}
}
