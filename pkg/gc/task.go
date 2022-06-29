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

//go:generate mockgen -destination runner_mock.go -source task.go -package gc

package gc

import (
	"errors"
	"time"
)

type Runner interface {
	RunGC() error
}

// Task is an struct used to run GC instance.
type Task struct {
	ID       string
	Interval time.Duration
	Timeout  time.Duration
	Runner   Runner
}

// Validate task params.
func (t *Task) validate() error {
	if t.ID == "" {
		return errors.New("empty ID is not specified")
	}

	if t.Interval <= 0 {
		return errors.New("Interval value is greater than 0")
	}

	if t.Timeout <= 0 {
		return errors.New("Timeout value is greater than 0")
	}

	if t.Timeout > t.Interval {
		return errors.New("Timeout value needs to be less than the Interval value")
	}

	if t.Runner == nil {
		return errors.New("empty Runner is not specified")
	}

	return nil
}
