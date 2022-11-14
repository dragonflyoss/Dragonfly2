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
	AttributePeerHost          = attribute.Key("d7y.peer.host")
	AttributeTaskID            = attribute.Key("d7y.peer.task.id")
	AttributeTaskContentLength = attribute.Key("d7y.peer.task.content_length")
	AttributePeerID            = attribute.Key("d7y.peer.id")
	AttributeTargetPeerID      = attribute.Key("d7y.peer.target.id")
	AttributeReusePeerID       = attribute.Key("d7y.peer.reuse.id")
	AttributeReuseRange        = attribute.Key("d7y.peer.reuse.range")
	AttributeTargetPeerAddr    = attribute.Key("d7y.peer.target.addr")
	AttributeMainPeer          = attribute.Key("d7y.peer.task.main_peer")
	AttributePeerPacketCode    = attribute.Key("d7y.peer.packet.code")
	AttributePeerTaskSizeScope = attribute.Key("d7y.peer.size.scope")
	AttributePeerTaskSize      = attribute.Key("d7y.peer.size")
	AttributePeerTaskSuccess   = attribute.Key("d7y.peer.task.success")
	AttributePeerTaskCode      = attribute.Key("d7y.peer.task.code")
	AttributePeerTaskMessage   = attribute.Key("d7y.peer.task.message")
	AttributePeerTaskCost      = attribute.Key("d7y.peer.task.cost")
	AttributePiece             = attribute.Key("d7y.peer.piece")
	AttributePieceSize         = attribute.Key("d7y.peer.piece.size")
	AttributePieceWorker       = attribute.Key("d7y.peer.piece.worker")
	AttributePieceSuccess      = attribute.Key("d7y.peer.piece.success")
	AttributeGetPieceStartNum  = attribute.Key("d7y.peer.piece.start")
	AttributeGetPieceLimit     = attribute.Key("d7y.peer.piece.limit")
	AttributeGetPieceCount     = attribute.Key("d7y.peer.piece.count")
	AttributeGetPieceRetry     = attribute.Key("d7y.peer.piece.retry")
	AttributeWritePieceSuccess = attribute.Key("d7y.peer.piece.write.success")
	AttributeSeedTaskSuccess   = attribute.Key("d7y.seed.task.success")

	SpanFileTask          = "file-task"
	SpanStreamTask        = "stream-task"
	SpanSeedTask          = "seed-task"
	SpanPeerTask          = "peer-task"
	SpanDownload          = "download"
	SpanRecursiveDownload = "recursive-download"
	SpanTransport         = "transport"
	SpanReusePeerTask     = "reuse-peer-task"
	SpanRegisterTask      = "register"
	SpanReportPeerResult  = "report-peer-result"
	SpanReportPieceResult = "report-piece-result"
	SpanBackSource        = "client-back-source"
	SpanFirstSchedule     = "schedule-#1"
	SpanGetPieceTasks     = "get-piece-tasks"
	SpanSyncPieceTasks    = "sync-piece-tasks"
	SpanDownloadPiece     = "download-piece-#%d"
	SpanProxy             = "proxy"
	SpanWritePiece        = "write-piece"
	SpanWriteBackPiece    = "write-back-piece"
	SpanWaitPieceLimit    = "wait-limit"
	SpanPeerGC            = "peer-gc"
)
