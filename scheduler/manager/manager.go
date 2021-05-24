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

package manager

import (
	"d7y.io/dragonfly/v2/scheduler/config"
)

type Manager struct {
	CDNManager  *CDNManager
	TaskManager *TaskManager
	HostManager *HostManager
}

func New(cfg *config.Config, dynconfig config.DynconfigInterface) (*Manager, error) {
	hostManager := newHostManager()
	taskManager := newTaskManager(cfg, hostManager)
	cdnManager, err := newCDNManager(cfg, taskManager, hostManager, dynconfig)
	if err != nil {
		return nil, err
	}

	return &Manager{
		CDNManager:  cdnManager,
		TaskManager: taskManager,
		HostManager: hostManager,
	}, nil
}
