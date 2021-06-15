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
//go:generate mockgen -destination ./mock/mock_progress_mgr.go -package mock d7y.io/dragonfly/v2/cdnsystem/daemon/mgr SeedProgressMgr

package daemon

import (
	"context"

	"d7y.io/dragonfly/v2/cdnsystem/types"
)

// SeedProgressMgr as an interface defines all operations about seed progress
type SeedProgressMgr interface {

	// InitSeedProgress init task seed progress
	InitSeedProgress(context.Context, string)

	// WatchSeedProgress watch task seed progress
	WatchSeedProgress(context.Context, string) (<-chan *types.SeedPiece, error)

	// PublishPiece publish piece seed
	PublishPiece(context.Context, string, *types.SeedPiece) error

	// PublishTask publish task seed
	PublishTask(context.Context, string, *types.SeedTask) error

	// GetPieces get pieces by taskID
	GetPieces(context.Context, string) (records []*types.SeedPiece, err error)

	// Clear meta info of task
	Clear(string) error

	SetTaskMgr(taskMgr SeedTaskMgr)
}
