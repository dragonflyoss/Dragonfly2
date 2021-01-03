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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/prometheus/client_golang/prometheus"
)

type CDNBuilder func(cfg *config.Config, cacheStore *store.Store,
	resourceClient source.ResourceClient, register prometheus.Registerer) (CDNMgr, error)

var cdnBuilderMap = make(map[config.CDNPattern]CDNBuilder)

func Register(name config.CDNPattern, builder CDNBuilder) {
	cdnBuilderMap[name] = builder
}

// get an implementation of the interface of CDNMgr
func GetCDNManager(cfg *config.Config, cacheStore *store.Store, resourceClient source.ResourceClient, register prometheus.Registerer) (CDNMgr, error) {
	cdnPattern := cfg.CDNPattern
	if cdnPattern.String() == "" {
		cdnPattern = config.CDNPatternLocal
	}

	cdnBuilder, ok := cdnBuilderMap[cdnPattern]
	if !ok {
		return nil, fmt.Errorf("unexpected cdn pattern(%s) which must be in [\"local\", \"source\"]", cdnPattern)
	}
	return cdnBuilder(cfg, cacheStore, resourceClient, register)
}

// CDNMgr as an interface defines all operations against CDN and
// operates on the underlying files stored on the local disk, etc.
type CDNMgr interface {

	// TriggerCDN will trigger CDN to download the file from sourceUrl.
	TriggerCDN(ctx context.Context, taskInfo *types.SeedTask) (*types.SeedTask, error)

	// GetHTTPPath returns the http download path of taskID.
	GetHTTPPath(ctx context.Context, taskInfo *types.SeedTask) (string, error)

	// GetGCTaskIDs returns the taskIDs that should exec GC operations as a string slice.
	// It should return nil when the free disk of cdn storage is lager than config.YoungGCThreshold.
	// It should return all taskIDs that are not running when the free disk of cdn storage is less than config.FullGCThreshold.
	GetGCTaskIDs(ctx context.Context, taskMgr SeedTaskMgr) ([]string, error)

	// CheckFile checks the file whether exists.
	CheckFileExist(ctx context.Context, taskID string) bool

	// Delete the cdn meta with specified taskID.
	// The file on the disk will be deleted when the force is true.
	Delete(ctx context.Context, taskID string) error

	// WatchSeedTask
	WatchSeedTask(taskID string, taskMgr SeedTaskMgr) (<-chan *types.SeedPiece, error)

}
