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
	AttributeObtainSeedsRequest   = attribute.Key("d7y.obtain.seeds.request")
	AttributeGetPieceTasksRequest = attribute.Key("d7y.get.piece.tasks.request")
	AttributePiecePacketResult    = attribute.Key("d7y.piece.packet.result")
	AttributeTaskID               = attribute.Key("d7y.task.id")
	AttributeTaskStatus           = attribute.Key("d7y.task.status")
	AttributeTaskURL              = attribute.Key("d7y.task.url")
	AttributeTaskInfo             = attribute.Key("d7y.taskInfo")
	AttributeIfReuseTask          = attribute.Key("d7y.task.already.exist")
	AttributeSeedPiece            = attribute.Key("d7y.seed.piece")
	AttributeSeedTask             = attribute.Key("d7y.seed.task")
	AttributeCacheResult          = attribute.Key("d7y.cache.result")
	AttributeWriteGoroutineCount  = attribute.Key("d7y.write.goroutine.count")
	AttributeDownloadFileInfo     = attribute.Key("d7y.download.file.info")
)

const (
	SpanObtainSeeds          = "cdn-obtain-seeds"
	SpanGetPieceTasks        = "get-piece-tasks"
	SpanTaskRegister         = "task-register"
	SpanAndOrUpdateTask      = "add-or-update-task"
	SpanTriggerCDNSyncAction = "trigger-cdn-sync-action"
	SpanTriggerCDN           = "trigger-cdn"
	SpanDetectCache          = "detect-cache"
	SpanDownloadSource       = "download-source"
	SpanWriteData            = "write-data"
)

const (
	EventHitUnReachableURL       = "hit-unReachableURL"
	EventRequestSourceFileLength = "request-source-file-length"
	EventDeleteUnReachableTask   = "downloaded"
	EventInitSeedProgress        = "init-seed-progress"
	EventWatchSeedProgress       = "watch-seed-progress"
	EventPublishPiece            = "publish-piece"
	EventPublishTask             = "publish-task"
)
