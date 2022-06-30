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

//go:generate mockgen -destination mocks/keepalive_mock.go -source keepalive.go -package mocks

package util

import (
	"time"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type KeepAlive interface {
	Keep()
	Alive(alive time.Duration) bool
}

type keepAlive struct {
	name   string
	access atomic.Int64
}

func NewKeepAlive(name string) KeepAlive {
	return &keepAlive{
		name: name,
	}
}

func (k *keepAlive) Keep() {
	k.access.Store(time.Now().UnixNano())
}

func (k *keepAlive) Alive(alive time.Duration) bool {
	var (
		now    = time.Now()
		access = time.Unix(0, k.access.Load())
	)

	logger.Debugf("%s keepalive check, last access: %s, alive time: %f seconds, current time: %s",
		k.name, access.Format(time.RFC3339), alive.Seconds(), now)
	return access.Add(alive).After(now)
}
