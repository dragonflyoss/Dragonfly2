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
	Register(ctx context.Context, registerRequest *types.TaskRegisterRequest) (pieceCh <-chan types.SeedPiece, err error)

	// Get the task Info with specified taskID.
	Get(ctx context.Context, taskID string) (*types.SeedTask, error)

	// GetAccessTime gets all task accessTime.
	GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error)

	// Delete deletes a task.
	Delete(ctx context.Context, taskID string) error
}
