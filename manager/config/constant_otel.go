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

package config

import "go.opentelemetry.io/otel/attribute"

const (
	AttributeID               = attribute.Key("d7y.manager.id")
	AttributePreheatType      = attribute.Key("d7y.manager.preheat.type")
	AttributePreheatURL       = attribute.Key("d7y.manager.preheat.url")
	AttributeDeleteTaskID     = attribute.Key("d7y.manager.delete_task.id")
	AttributeListTasksID      = attribute.Key("d7y.manager.list_tasks.id")
	AttributeListTasksPage    = attribute.Key("d7y.manager.list_tasks.page")
	AttributeListTasksPerPage = attribute.Key("d7y.manager.list_tasks.per_page")
)

const (
	SpanPreheat          = "preheat"
	SpanSyncPeers        = "sync-peers"
	SpanGetLayers        = "get-layers"
	SpanAuthWithRegistry = "auth-with-registry"
	SpanDeleteTask       = "delete-task"
	SpanListTasks        = "list-tasks"
)
