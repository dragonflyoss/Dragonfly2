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

package clientutil

import (
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

type KeepAlive interface {
	Keep()
	Alive(alive time.Duration) bool
}

type keepAlive struct {
	name   string
	access time.Time
}

var _ KeepAlive = (*keepAlive)(nil)

func NewKeepAlive(name string) KeepAlive {
	return &keepAlive{
		name:   name,
		access: time.Now(),
	}
}

func (k keepAlive) Keep() {
	k.access = time.Now()
	logger.Debugf("update %s keepalive access time: %s", k.name, k.access.Format(time.RFC3339))
}

func (k keepAlive) Alive(alive time.Duration) bool {
	var now = time.Now()
	logger.Debugf("%s keepalive check, last access: %s, alive time: %f seconds, current time: %s",
		k.name, k.access.Format(time.RFC3339), alive.Seconds(), now)
	return k.access.Add(alive).After(now)
}
