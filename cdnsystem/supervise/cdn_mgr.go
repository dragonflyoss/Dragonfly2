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

//go:generate mockgen -destination ./mock/mock_cdn_mgr.go -package mock d7y.io/dragonfly/v2/cdnsystem/supervise CDNMgr

package supervise

import (
	"context"

	"d7y.io/dragonfly/v2/cdnsystem/types"
)

// CDNMgr as an interface defines all operations against CDN and
// operates on the underlying files stored on the local disk, etc.
type CDNMgr interface {

	// TriggerCDN will trigger CDN to download the resource from sourceUrl.
	TriggerCDN(context.Context, *types.SeedTask) (*types.SeedTask, error)

	// Delete the cdn meta with specified taskID.
	// The file on the disk will be deleted when the force is true.
	Delete(string) error
}
