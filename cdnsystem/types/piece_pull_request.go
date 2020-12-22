package types


type PiecePullRequest struct {

	TaskID string `json:"taskId,omitempty"`

	// the uploader peerID
	//
	DstPID string `json:"dstPID,omitempty"`

	// the range of specific piece in the task, example "0-45565".
	//
	PieceRange string `json:"pieceRange,omitempty"`

	// pieceResult It indicates whether the dfgetTask successfully download the piece.
	// It's only useful when `status` is `RUNNING`.
	//
	// Enum: [FAILED SUCCESS INVALID SEMISUC]
	PieceResult string `json:"pieceResult,omitempty"`
}
