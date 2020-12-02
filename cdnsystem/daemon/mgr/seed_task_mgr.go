package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/syncmap"
)

// SeedTaskMgr as an interface defines all operations against SeedTask.
// A SeedTask will store some meta info about the taskFile, pieces and something else.
// A SeedTask has a one-to-one correspondence with a file on the disk which is identified by taskID.
type SeedTaskMgr interface {

	Register(ctx context.Context, registerRequest *types.TaskRegisterRequest) (taskCreateResponse *types.TaskRegisterResponse, err error)

	// Get the task Info with specified taskID.
	Get(ctx context.Context, taskID string) (*types.SeedTaskInfo, error)

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
