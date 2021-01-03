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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
)

// SeedProgressMgr as an interface defines all operations about seed progress
type SeedProgressMgr interface {

	// WatchSeedProgress watch seed progress
	WatchSeedProgress(taskID string, taskMgr SeedTaskMgr) (<-chan *types.SeedPiece, error)

	// UnWatchSeedProgress unwatch seed progress
	UnWatchSeedProgress(seedSubscriber chan *types.SeedPiece, taskID string) error

	// PublishPiece publish seedPiece
	PublishPiece(taskID string, record *types.SeedPiece) error

	// GetPieceMetaRecordsByTaskID
	GetPieceMetaRecordsByTaskID(taskID string) (records []*types.SeedPiece, err error)

	Close(taskID string) error
}