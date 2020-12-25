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

package cdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/prometheus/client_golang/prometheus"
)

var _ mgr.CDNMgr = &Manager{}

func init() {
	mgr.Register(config.CDNPatternSource, NewManager)
}

// Manager is an implementation of the interface of CDNMgr which use source as CDN node.
type Manager struct {
	cfg *config.Config
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore *store.Store, resourceClient source.ResourceClient, register prometheus.Registerer) (mgr.CDNMgr, error) {
	return &Manager{
		cfg: cfg,
	}, nil
}

// TriggerCDN will trigger CDN to download the file from sourceUrl.
func (cm *Manager) TriggerCDN(ctx context.Context, task *types.SeedTaskInfo) (*types.SeedTaskInfo, error) {
	sourceFileLength := task.SourceFileLength
	if sourceFileLength == 0 {
		sourceFileLength = -1
	}

	if sourceFileLength > 0 {
		pieceTotal := int((sourceFileLength + int64(task.PieceSize-1)) / int64(task.PieceSize))

		var pieceNum int
		for pieceNum = 0; pieceNum < pieceTotal; pieceNum++ {
			//pieceChan <- &pb.PieceSeed{
			//	//todo 上报
			//}
		}

	}
	return &types.SeedTaskInfo{
		CdnStatus: types.TaskInfoCdnStatusSUCCESS,
	}, nil
}

// GetHTTPPath returns the http download path of taskID.
func (cm *Manager) GetHTTPPath(ctx context.Context, taskInfo *types.SeedTaskInfo) (string, error) {
	return taskInfo.Url, nil
}

// GetStatus gets the status of the file.
func (cm *Manager) GetStatus(ctx context.Context, taskID string) (cdnStatus string, err error) {
	return "", nil
}

// GetPieceMD5 gets the piece Md5 accorrding to the specified taskID and pieceNum.
func (cm *Manager) GetPieceMD5(ctx context.Context, taskID string, pieceNum int, pieceRange, source string) (pieceMd5 string, err error) {
	return "", nil
}

// CheckFile checks the file whether exists.
func (cm *Manager) CheckFileExist(ctx context.Context, taskID string) bool {
	return true
}

// Delete the cdn meta with specified taskID.
// It will also delete the files on the disk when the force equals true.
func (cm *Manager) Delete(ctx context.Context, taskID string, force bool) error {
	return nil
}

func (cm *Manager) GetGCTaskIDs(ctx context.Context, taskMgr mgr.SeedTaskMgr) ([]string, error) {
	return nil, nil
}
