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

package daemon

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/scheduler/types"
)

type CDNMgr interface {
	// GetLastTriggerSeedTime
	GetLastTriggerSeedTime(taskID string) time.Time
	// SeedTask
	SeedTask(ctx context.Context, task *types.Task) (peerNode *types.PeerNode, err error)
}
