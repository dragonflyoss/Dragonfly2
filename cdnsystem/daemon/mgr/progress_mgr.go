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
	"d7y.io/dragonfly/v2/cdnsystem/types"
)

// SeedProgressMgr as an interface defines all operations about seed progress
type SeedProgressMgr interface {

	// InitSeedProgress init task seed progress
	InitSeedProgress(ctx context.Context, taskID string) error

	// WatchSeedProgress watch task seed progress
	WatchSeedProgress(ctx context.Context, taskID string) (<-chan *types.SeedPiece, error)

	// UnWatchSeedProgress unwatch task seed progress
	UnWatchSeedProgress(seedSubscriber chan *types.SeedPiece, taskID string) error

	// PublishPiece publish piece seed
	PublishPiece(taskID string, record *types.SeedPiece) error

	// PublishTask publish task seed
	PublishTask(taskID string, record *types.SeedPiece) error

	// GetPieceMetaRecordsByTaskID get pieces by taskId
	GetPieceMetaRecordsByTaskID(taskID string) (records []*types.SeedPiece, err error)

	// Clear
	Clear(taskID string) error
}