/*
 *     Copyright 2024 The Dragonfly Authors
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

//go:generate mockgen -destination resource_mock.go -source resource.go -package persistentcache

package persistentcache

import (
	"github.com/redis/go-redis/v9"

	"d7y.io/dragonfly/v2/scheduler/config"
)

// Resource is the interface used for resource.
type Resource interface {
	// Host manager interface.
	HostManager() HostManager

	// Peer manager interface.
	PeerManager() PeerManager

	// Task manager interface.
	TaskManager() TaskManager
}

// resource contains content for resource.
type resource struct {
	// Peer manager interface.
	peerManager PeerManager

	// Task manager interface.
	taskManager TaskManager

	// Host manager interface.
	hostManager HostManager
}

// New returns Resource interface.
func New(cfg *config.Config, rdb redis.UniversalClient) Resource {
	taskManager := newTaskManager(cfg, rdb)
	hostManager := newHostManager(cfg, rdb)
	peerManager := newPeerManager(cfg, rdb, taskManager, hostManager)
	return &resource{peerManager, taskManager, hostManager}
}

// Host manager interface.
func (r *resource) HostManager() HostManager {
	return r.hostManager
}

// Peer manager interface.
func (r *resource) PeerManager() PeerManager {
	return r.peerManager
}

// Task manager interface.
func (r *resource) TaskManager() TaskManager {
	return r.taskManager
}
