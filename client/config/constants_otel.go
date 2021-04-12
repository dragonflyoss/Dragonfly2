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

	SpanFilePeerTask   = "file-peer-task"
	SpanStreamPeerTask = "stream-peer-task"
	SpanRegisterTask   = "register"
	SpanFirstSchedule  = "schedule-#1"
	SpanGetPieceTasks  = "get-piece-tasks"
	SpanDownloadPiece  = "download-piece-#%d"
	SpanWaitPieceLimit = "wait-limit"
	SpanPeerGC         = "peer-gc"
)
