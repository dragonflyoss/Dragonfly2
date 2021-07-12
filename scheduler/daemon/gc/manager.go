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
	"time"

	"d7y.io/dragonfly/v2/scheduler/daemon"
)

type manager struct {
	taskManager  daemon.TaskMgr
	gcInterval   time.Duration
	initialDelay time.Duration
	done         chan bool
}

//
//func newManager(cfg *config.GCConfig) daemon.GCMgr {
//	return &manager{
//		gcInterval: time.Duration(3),
//		done:       nil,
//	}
//}

func (m *manager) StartGC(ctx context.Context) error {
	time.Sleep(m.initialDelay)
	return nil

}
