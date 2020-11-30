package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/syncmap"
)

// PieceStatusMap maintains the mapping relationship between PieceUpdateRequestResult and PieceStatus code.
var PieceStatusMap = map[string]int{
	types.PieceUpdateRequestPieceStatusFAILED:  config.PieceFAILED,
	types.PieceUpdateRequestPieceStatusSEMISUC: config.PieceSEMISUC,
	types.PieceUpdateRequestPieceStatusSUCCESS: config.PieceSUCCESS,
}

// TaskMgr as an interface defines all operations against CdnTask.
// A CdnTask will store some meta info about the taskFile, pieces and something else.
// A CdnTask has a one-to-one correspondence with a file on the disk which is identified by taskID.
type TaskMgr interface {
	// add or update task with req
	AddOrUpdateTask(ctx context.Context, req *types.CdnTaskCreateRequest) (*types.CdnTaskInfo, error)

 	TriggerCdnSyncAction(ctx context.Context, task *types.CdnTaskInfo) error

	// Get the task Info with specified taskID.
	Get(ctx context.Context, taskID string) (*types.CdnTaskInfo, error)

	// GetAccessTime gets all task accessTime.
	GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error)

	// Delete deletes a task.
	Delete(ctx context.Context, taskID string) error

	// GetPieces gets the pieces to be downloaded based on the scheduling result,
	// just like this: which pieces can be downloaded from which peers.
	GetPieces(ctx context.Context, taskID, clientID string, piecePullRequest *types.PiecePullRequest) (isFinished bool, data interface{}, err error)

	// UpdatePieceStatus updates the piece status with specified parameters.
	// A task file is divided into several pieces logically.
	// We use a sting called pieceRange to identify a piece.
	// A pieceRange is separated by a dash, like this: 0-45565, etc.
	UpdatePieceStatus(ctx context.Context, taskID, pieceRange string, pieceUpdateRequest *types.PieceUpdateRequest) error
}
