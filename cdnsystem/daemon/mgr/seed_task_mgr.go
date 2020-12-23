package mgr

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
)

// SeedTaskMgr as an interface defines all operations against SeedTask.
// A SeedTask will store some meta info about the taskFile, pieces and something else.
// A seedTask corresponds to three files on the disk, which are identified by taskId, the data file meta file piece file
type SeedTaskMgr interface {

	// Register the seed task
	Register(ctx context.Context, registerRequest *types.TaskRegisterRequest) error

	// Get the task Info with specified taskID.
	Get(ctx context.Context, taskID string) (*types.SeedTaskInfo, error)

	// GetAccessTime gets all task accessTime.
	GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error)

	// Delete deletes a task.
	Delete(ctx context.Context, taskID string) error

	// GetPieces
	GetPieces(ctx context.Context, piecePullRequest *types.PiecePullRequest) (pieceCh <-chan types.SeedPiece, err error)

}
