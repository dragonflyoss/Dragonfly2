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
	"time"

	"context"
	"crypto/md5"
	"fmt"

	"d7y.io/dragonfly/v2/cdnsystem/daemon"
	_ "d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage/disk"   // To register diskStorage
	_ "d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage/hybrid" // To register hybridStorage
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/limitreader"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/ratelimiter"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// Ensure that Manager implements the CDNMgr interface
var _ daemon.CDNMgr = (*Manager)(nil)

// Manager is an implementation of the interface of CDNMgr.
type Manager struct {
	cfg              *config.Config
	cacheStore       storage.Manager
	limiter          *ratelimiter.RateLimiter
	cdnLocker        *synclock.LockerPool
	cacheDataManager *cacheDataManager
	progressMgr      daemon.SeedProgressMgr
	cdnReporter      *reporter
	detector         *cacheDetector
	writer           *cacheWriter
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore storage.Manager, progressMgr daemon.SeedProgressMgr) (daemon.CDNMgr, error) {
	return newManager(cfg, cacheStore, progressMgr)
}

func newManager(cfg *config.Config, cacheStore storage.Manager, progressMgr daemon.SeedProgressMgr) (*Manager, error) {
	rateLimiter := ratelimiter.NewRateLimiter(ratelimiter.TransRate(int64(cfg.MaxBandwidth-cfg.SystemReservedBandwidth)), 2)
	cacheDataManager := newCacheDataManager(cacheStore)
	cdnReporter := newReporter(progressMgr)
	return &Manager{
		cfg:              cfg,
		cacheStore:       cacheStore,
		limiter:          rateLimiter,
		cdnLocker:        synclock.NewLockerPool(),
		cacheDataManager: cacheDataManager,
		cdnReporter:      cdnReporter,
		progressMgr:      progressMgr,
		detector:         newCacheDetector(cacheDataManager),
		writer:           newCacheWriter(cdnReporter, cacheDataManager),
	}, nil
}

func (cm *Manager) TriggerCDN(ctx context.Context, task *types.SeedTask) (seedTask *types.SeedTask, err error) {
	// obtain taskId write lock
	cm.cdnLocker.Lock(task.TaskID, false)
	defer cm.cdnLocker.UnLock(task.TaskID, false)
	// first: detect Cache
	detectResult, err := cm.detector.detectCache(task)
	if err != nil {
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFailed), errors.Wrapf(err, "detect task %s cache", task.TaskID)
	}
	logger.WithTaskID(task.TaskID).Debugf("detects cache result: %+v", detectResult)
	// second: report detect result
	err = cm.cdnReporter.reportCache(task.TaskID, detectResult)
	if err != nil {
		logger.WithTaskID(task.TaskID).Errorf("failed to report cache, reset detectResult:%v", err)
	}
	// full cache
	if detectResult.breakPoint == -1 {
		logger.WithTaskID(task.TaskID).Infof("cache full hit on local")
		return getUpdateTaskInfo(types.TaskInfoCdnStatusSuccess, detectResult.fileMetaData.SourceRealMd5, detectResult.fileMetaData.PieceMd5Sign,
			detectResult.fileMetaData.SourceFileLen, detectResult.fileMetaData.CdnFileLength), nil
	}
	server.StatSeedStart(task.TaskID, task.URL)
	start := time.Now()
	// third: start to download the source file
	body, expireInfo, err := cm.download(ctx, task, detectResult)
	// download fail
	if err != nil {
		server.StatSeedFinish(task.TaskID, task.URL, false, err, start.Nanosecond(), time.Now().Nanosecond(), 0, 0)
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusSourceError), err
	}
	defer body.Close()

	// update Expire info
	cm.updateExpireInfo(task.TaskID, expireInfo)
	fileMd5 := md5.New()
	if detectResult.fileMd5 != nil {
		fileMd5 = detectResult.fileMd5
	}
	reader := limitreader.NewLimitReaderWithLimiterAndMD5Sum(body, cm.limiter, fileMd5)
	// forth: write to storage
	downloadMetadata, err := cm.writer.startWriter(reader, task, detectResult)
	if err != nil {
		server.StatSeedFinish(task.TaskID, task.URL, false, err, start.Nanosecond(), time.Now().Nanosecond(), downloadMetadata.backSourceLength,
			downloadMetadata.realSourceFileLength)
		logger.WithTaskID(task.TaskID).Errorf("failed to write for task: %v", err)
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFailed), err
	}
	server.StatSeedFinish(task.TaskID, task.URL, true, nil, start.Nanosecond(), time.Now().Nanosecond(), downloadMetadata.backSourceLength,
		downloadMetadata.realSourceFileLength)
	sourceMD5 := reader.Md5()
	// fifth: handle CDN result
	success, err := cm.handleCDNResult(ctx, task, sourceMD5, downloadMetadata)
	if err != nil || !success {
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFailed), err
	}
	return getUpdateTaskInfo(types.TaskInfoCdnStatusSuccess, sourceMD5, downloadMetadata.pieceMd5Sign,
		downloadMetadata.realSourceFileLength, downloadMetadata.realCdnFileLength), nil
}

func (cm *Manager) Delete(taskID string) error {
	err := cm.cacheStore.DeleteTask(taskID)
	if err != nil {
		return errors.Wrap(err, "failed to delete task files")
	}
	return nil
}

func (cm *Manager) handleCDNResult(ctx context.Context, task *types.SeedTask, sourceMd5 string, downloadMetadata *downloadMetadata) (bool, error) {
	logger.WithTaskID(task.TaskID).Debugf("handle cdn result, downloadMetaData: %+v", downloadMetadata)
	var isSuccess = true
	var errorMsg string
	// check md5
	if !stringutils.IsBlank(task.RequestMd5) && task.RequestMd5 != sourceMd5 {
		errorMsg = fmt.Sprintf("file md5 not match expected:%s real:%s", task.RequestMd5, sourceMd5)
		isSuccess = false
	}
	// check source length
	if isSuccess && task.SourceFileLength >= 0 && task.SourceFileLength != downloadMetadata.realSourceFileLength {
		errorMsg = fmt.Sprintf("file length not match expected:%d real:%d", task.SourceFileLength, downloadMetadata.realSourceFileLength)
		isSuccess = false
	}
	if isSuccess && task.PieceTotal > 0 && downloadMetadata.pieceTotalCount != task.PieceTotal {
		errorMsg = fmt.Sprintf("task total piece count not match expected:%d real:%d", task.PieceTotal, downloadMetadata.pieceTotalCount)
		isSuccess = false
	}
	if !stringutils.IsBlank(errorMsg) {
		logger.WithTaskID(task.TaskID).Error(errorMsg)
	}
	sourceFileLen := task.SourceFileLength
	if isSuccess && task.SourceFileLength <= 0 {
		sourceFileLen = downloadMetadata.realSourceFileLength
	}
	cdnFileLength := downloadMetadata.realCdnFileLength
	pieceMd5Sign := downloadMetadata.pieceMd5Sign
	// if validate fail
	if !isSuccess {
		cdnFileLength = 0
	}
	if err := cm.cacheDataManager.updateStatusAndResult(task.TaskID, &storage.FileMetaData{
		Finish:          true,
		Success:         isSuccess,
		SourceRealMd5:   sourceMd5,
		PieceMd5Sign:    pieceMd5Sign,
		CdnFileLength:   cdnFileLength,
		SourceFileLen:   sourceFileLen,
		TotalPieceCount: downloadMetadata.pieceTotalCount,
	}); err != nil {
		return false, errors.Wrap(err, "failed to update task status and result")
	}

	if !isSuccess {
		return false, errors.New(errorMsg)
	}

	logger.WithTaskID(task.TaskID).Infof("success to get task, downloadMetadata:%+v realMd5: %s", downloadMetadata, sourceMd5)

	return true, nil
}

func (cm *Manager) updateExpireInfo(taskID string, expireInfo map[string]string) {
	if err := cm.cacheDataManager.updateExpireInfo(taskID, expireInfo); err != nil {
		logger.WithTaskID(taskID).Errorf("failed to update expireInfo(%s): %v", expireInfo, err)
	}
	logger.WithTaskID(taskID).Infof("success to update expireInfo(%s)", expireInfo)
}

/*
	helper functions
*/
var getCurrentTimeMillisFunc = timeutils.CurrentTimeMillis

// getUpdateTaskInfoWithStatusOnly
func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.SeedTask {
	return getUpdateTaskInfo(cdnStatus, "", "", 0, 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5, pieceMd5Sign string, sourceFileLength, cdnFileLength int64) *types.SeedTask {
	return &types.SeedTask{
		CdnStatus:        cdnStatus,
		PieceMd5Sign:     pieceMd5Sign,
		SourceRealMd5:    realMD5,
		SourceFileLength: sourceFileLength,
		CdnFileLength:    cdnFileLength,
	}
}
