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

package job

// Queue Name.
const (
	GlobalQueue     = Queue("global")
	SchedulersQueue = Queue("schedulers")
)

// Job Name.
const (
	// PreheatJob is the name of preheat job.
	PreheatJob = "preheat"

	// SyncPeersJob is the name of syncing peers job.
	SyncPeersJob = "sync_peers"

	// ListTasksJob is the name of listing tasks job.
	ListTasksJob = "list_tasks"

	// DeleteTasksJob is the name of deleting tasks job.
	DeleteTaskJob = "delete_task"
)

// Machinery server configuration.
const (
	DefaultResultsExpireIn     = 86400
	DefaultRedisMaxIdle        = 10
	DefaultRedisIdleTimeout    = 300
	DefaultRedisReadTimeout    = 60
	DefaultRedisWriteTimeout   = 60
	DefaultRedisConnectTimeout = 60
)
