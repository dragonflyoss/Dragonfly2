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
	AttributePeerRegisterRequest = attribute.Key("d7y.peer.register.request")
	AttributeTaskSizeScope       = attribute.Key("d7y.task.size.scope")
	AttributeSinglePiece         = attribute.Key("d7y.peer.single.piece")
	AttributePieceReceived       = attribute.Key("d7y.peer.piece.received")
	AttributeLeavePeerID         = attribute.Key("d7y.leave.peer.id")
	AttributeLeaveTaskID         = attribute.Key("d7y.leave.task.id")
	AttributeReportPeerID        = attribute.Key("d7y.report.peer.id")
	AttributePeerDownloadSuccess = attribute.Key("d7y.peer.download.success")
	AttributeDownloadFileURL     = attribute.Key("d7y.file.url")
	AttributeContentLength       = attribute.Key("d7y.source.content.length")
	AttributePeerDownloadResult  = attribute.Key("d7y.peer.download.result")
	AttributeSchedulePacket      = attribute.Key("d7y.schedule.packet")
	AttributeTaskID              = attribute.Key("d7y.peer.task.id")
	AttributePeerID              = attribute.Key("d7y.peer.id")
	AttributeCDNSeedRequest      = attribute.Key("d7y.cdn.seed.request")
	AttributeNeedSeedCDN         = attribute.Key("d7y.need.seed.cdn")
	AttributeTaskStatus          = attribute.Key("d7y.task.status")
	AttributeLastTriggerTime     = attribute.Key("d7y.task.last.trigger.time")
	AttributeClientBackSource    = attribute.Key("d7y.need.client.back-source")
	AttributeTriggerCDNError     = attribute.Key("d7y.trigger.cdn.error")
)

const (
	SpanPeerRegister      = "peer-register"
	SpanTriggerCDNSeed    = "trigger-cdn-seed"
	SpanReportPieceResult = "report-piece-result"
	SpanReportPeerResult  = "report-peer-result"
	SpanPeerLeave         = "peer-leave"
)

const (
	EventSmallTaskSelectParentFail = "small-task-select-parent-fail"
	EventPeerNotFound              = "peer-not-found"
	EventHostNotFound              = "host-not-found"
	EventCreateCDNPeer             = "create-cdn-peer"
	EventCDNPieceReceived          = "receive-cdn-piece"
	EventPeerDownloaded            = "downloaded"
	EventDownloadTinyFile          = "download-tiny-file"
	EventCDNFailBackClientSource   = "cdn-fail-back-client-source"
)
