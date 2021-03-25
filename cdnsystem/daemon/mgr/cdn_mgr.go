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


// CDNMgr as an interface defines all operations against CDN and
// operates on the underlying files stored on the local disk, etc.
type CDNMgr interface {

	// TriggerCDN will trigger CDN to download the file from sourceUrl.
	TriggerCDN(ctx context.Context, taskInfo *types.SeedTask) (*types.SeedTask, error)

	// GetGCTaskIds returns the taskIds that should exec GC operations as a string slice.
	// It should return nil when the free disk of cdn storage is lager than config.YoungGCThreshold.
	// It should return all taskIds that are not running when the free disk of cdn storage is less than config.
	//FullGCThreshold.
	GetGCTaskIds(ctx context.Context, taskMgr SeedTaskMgr) ([]string, error)

	// Delete the cdn meta with specified taskID.
	// The file on the disk will be deleted when the force is true.
	Delete(ctx context.Context, taskID string, force bool) error
}
