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
	AttributeDownloadFileURL      = attribute.Key("d7y.file.url")
	AttributeContentLength        = attribute.Key("d7y.source.content.length")
	AttributePeerDownloadResult   = attribute.Key("d7y.peer.download.result")
	AttributeSchedulePacket       = attribute.Key("d7y.schedule.packet")
	AttributeTaskID               = attribute.Key("d7y.task.id")
	AttributeTaskStatus           = attribute.Key("")
	AttributeTaskURL              = attribute.Key("d7y.task.url")
	AttributeTaskInfo             = attribute.Key("d7y.taskInfo")
	AttributeIfReuseTask          = attribute.Key("")
	AttributeSeedPiece            = attribute.Key("")
	AttributeSourceFileLength     = attribute.Key("")
	AttributePeerID               = attribute.Key("d7y.peer.id")
	AttributeCDNSeedRequest       = attribute.Key("d7y.cdn.seed.request")
)

const (
	SpanObtainSeeds          = "cdn-obtain-seeds"
	SpanGetPieceTasks        = "get-piece-tasks"
	SpanTaskRegister         = "task-register"
	SpanAndOrUpdateTask      = "add-or-update-task"
	SpanTriggerCDNSyncAction = "trigger-cdn-sync-action"
	SpanTriggerCDN           = "trigger-cdn"
	SpanDetectCache          = "detect-cache"
	SpanInitSeedProgress     = "init-seed-progress"
	SpanReportPieceResult    = "report-piece-result"
	SpanReportPeerResult     = "report-peer-result"
	SpanPeerLeave            = "peer-leave"
)

const (
	EventHitUnReachableURL       = "hit-unReachableURL"
	EventPieceReceived           = "receive-piece"
	EventRequestSourceFileLength = "request-source-file-length"
	EventDeleteUnReachableTask   = "downloaded"
	EventUpdateTaskStatus        = "update-task-status"
	EventInitSeedProgress        = "init-seed-progress"
	EventWatchSeedProgress       = "watch-seed-progress"
	EventPublishPiece            = "publish-piece"
	EventPublishTask             = "publish-task"
)
