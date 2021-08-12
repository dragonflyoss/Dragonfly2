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
	AttributeLeavePeerID         = attribute.Key("d7y.leave.peer.id")
	AttributeLeaveTaskID         = attribute.Key("d7y.leave.task.id")
	AttributeReportPeerID        = attribute.Key("d7y.report.peer.id")
	AttributePeerDownloadSuccess = attribute.Key("d7y.peer.download.success")
	AttributePeerDownloadResult  = attribute.Key("d7y.peer.download.result")
	AttributeSchedulePacket      = attribute.Key("d7y.schedule.packet")
	AttributeTaskID              = attribute.Key("d7y.peer.task.id")
	AttributePeerID              = attribute.Key("d7y.peer.id")
)

const (
	SpanPeerRegister      = "peer-register"
	SpanTriggerCDN        = "trigger-cdn"
	SpanReportPieceResult = "report-piece-result"
	SpanReportPeerResult  = "report-peer-result"
	SpanPeerLeave         = "peer-leave"
)

const (
	EventScheduleParentFail = "fail-schedule-parent"
)
