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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type manager struct {
	taskManager   daemon.TaskMgr
	gcInterval      time.Duration
	initialDelay time.Duration
	done          chan bool
}

func newManager(cfg *config.GCConfig) daemon.GCMgr {
	return &manager{
		interval: 0,
		done:     nil,
	}
}

func (m *manager) StartGC(ctx context.Context) error {
	time.Sleep(m.initialDelay)
	ticker := time.NewTicker(m.gcInterval)
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


	m.taskManager.ListTasks().Range(func(taskID, value interface{}) bool {
		task := value.(*types.Task)
		if task.LastAccessTime.Add(task.TaskID).Before(time.Now())
	})
}
