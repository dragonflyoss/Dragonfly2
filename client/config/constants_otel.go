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
	AttributeTaskId            = attribute.Key("d7y.peer.task.id")
	AttributePeerId            = attribute.Key("d7y.peer.id")
	AttributeTargetPeerId      = attribute.Key("d7y.peer.target.id")
	AttributeMainPeer          = attribute.Key("d7y.peer.task.main-peer")
	AttributePeerTaskSizeScope = attribute.Key("d7y.peer.size.scope")
	AttributePeerTaskSuccess   = attribute.Key("d7y.peer.task.success")
	AttributePiece             = attribute.Key("d7y.peer.piece")
	AttributePieceWorker       = attribute.Key("d7y.peer.piece.worker")
	AttributePieceSuccess      = attribute.Key("d7y.peer.piece.success")
	AttributeGetPieceStartNum  = attribute.Key("d7y.peer.piece.start")
	AttributeGetPieceLimit     = attribute.Key("d7y.peer.piece.limit")
	AttributeGetPieceCount     = attribute.Key("d7y.peer.piece.count")
	AttributeGetPieceRetry     = attribute.Key("d7y.peer.piece.retry")
	AttributeWritePieceSuccess = attribute.Key("d7y.peer.piece.write.success")

	SpanFilePeerTask    = "file-peer-task"
	SpanStreamPeerTask  = "stream-peer-task"
	SpanRegisterTask    = "register"
	SpanFirstSchedule   = "schedule-#1"
	SpanGetPieceTasks   = "get-piece-tasks"
	SpanDownloadPiece   = "download-piece-#%d"
	SpanWritePiece      = "write-piece"
	SpanWriteBackPiece  = "write-back-piece"
	SpanWaitPieceLimit  = "wait-limit"
	SpanPushPieceResult = "push-result"
	SpanPeerGC          = "peer-gc"
)
