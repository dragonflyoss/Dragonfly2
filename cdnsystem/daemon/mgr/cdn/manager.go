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
	_ "d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage/disk"   // To register diskStorage
	_ "d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage/hybrid" // To register hybridStorage
)
import (
	"context"
	"crypto/md5"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rate/limitreader"
	"d7y.io/dragonfly/v2/pkg/rate/ratelimiter"
	"d7y.io/dragonfly/v2/pkg/util/metricsutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	// Ensure that Manager implements the CDNMgr interface
	var manager *Manager = nil
	var _ mgr.CDNMgr = manager
}

type metrics struct {
	cdnCacheHitCount     *prometheus.CounterVec
	cdnDownloadCount     *prometheus.CounterVec
	cdnDownloadBytes     *prometheus.CounterVec
	cdnDownloadFailCount *prometheus.CounterVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		cdnCacheHitCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_cache_hit_total",
			"Total times of hitting cdn cache", []string{}, register),

		cdnDownloadCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_download_total",
			"Total times of cdn download", []string{}, register),

		cdnDownloadBytes: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_download_size_bytes_total",
			"total file size of cdn downloaded from source in bytes", []string{}, register,
		),
		cdnDownloadFailCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_download_failed_total",
			"Total failure times of cdn download", []string{}, register),
	}
}

// Manager is an implementation of the interface of CDNMgr.
type Manager struct {
	cfg             *config.Config
	cacheStore      storage.StorageMgr
	limiter         *ratelimiter.RateLimiter
	cdnLocker       *util.LockerPool
	metaDataManager *metaDataManager
	progressMgr     mgr.SeedProgressMgr
	cdnReporter     *reporter
	detector        *cacheDetector
	resourceClient  source.ResourceClient
	writer          *cacheWriter
	metrics         *metrics
}

func (cm *Manager) GetGCTaskIds(ctx context.Context, taskMgr mgr.SeedTaskMgr) ([]string, error) {
	panic("implement me")
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore storage.StorageMgr, progressMgr mgr.SeedProgressMgr,
	resourceClient source.ResourceClient,
	register prometheus.Registerer) (mgr.CDNMgr, error) {
	rateLimiter := ratelimiter.NewRateLimiter(ratelimiter.TransRate(int64(cfg.MaxBandwidth-cfg.SystemReservedBandwidth)), 2)
	metaDataManager := newFileMetaDataManager(cacheStore)
	cdnReporter := newReporter(progressMgr, cacheStore)
	return &Manager{
		cfg:             cfg,
		cacheStore:      cacheStore,
		limiter:         rateLimiter,
		cdnLocker:       util.NewLockerPool(),
		metaDataManager: metaDataManager,
		cdnReporter:     cdnReporter,
		progressMgr:     progressMgr,
		detector:        newCacheDetector(cacheStore, metaDataManager, resourceClient),
		resourceClient:  resourceClient,
		writer:          newCacheWriter(cacheStore, cdnReporter, metaDataManager),
		metrics:         newMetrics(register),
	}, nil
}

func (cm *Manager) TriggerCDN(ctx context.Context, task *types.SeedTask) (seedTask *types.SeedTask, err error) {
	// obtain taskId write lock
	cm.cdnLocker.GetLock(task.TaskId, false)
	defer cm.cdnLocker.ReleaseLock(task.TaskId, false)
	// first: detect Cache
	detectResult, err := cm.detector.detectCache(ctx, task)
	logger.WithTaskID(task.TaskId).Debugf("detects cache result:%v", detectResult)
	if err != nil {
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), errors.Wrapf(err,
			"failed to detect cache")
	} else {
		logger.WithTaskID(task.TaskId).Debugf("start to update access time")
		if err := cm.metaDataManager.updateAccessTime(ctx, task.TaskId, getCurrentTimeMillisFunc()); err != nil {
			logger.WithTaskID(task.TaskId).Warnf("failed to update task access time ")
		}
	}
	// second: report detect result
	err = cm.cdnReporter.reportCache(ctx, task.TaskId, detectResult)
	if err != nil {
		logger.WithTaskID(task.TaskId).Errorf("failed to report cache, reset detectResult:%v", err)
	}
	// full cache
	if detectResult.breakNum == -1 {
		logger.WithTaskID(task.TaskId).Infof("cache full hit on local")
		cm.metrics.cdnCacheHitCount.WithLabelValues().Inc()
		return getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, detectResult.fileMetaData.SourceRealMd5,
			detectResult.fileMetaData.PieceMd5Sign, detectResult.fileMetaData.SourceFileLen,
			detectResult.fileMetaData.CdnFileLength), nil
	}
	// third: start to download the source file
	resp, err := cm.download(task, detectResult)
	cm.metrics.cdnDownloadCount.WithLabelValues().Inc()
	// download fail
	if err != nil {
		cm.metrics.cdnDownloadFailCount.WithLabelValues().Inc()
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusSourceERROR), err
	}
	defer resp.Body.Close()

	// update Expire info
	cm.updateExpireInfo(ctx, task.TaskId, resp.ExpireInfo)
	fileMd5 := md5.New()
	if detectResult.fileMd5 != nil {
		fileMd5 = detectResult.fileMd5
	}
	reader := limitreader.NewLimitReaderWithLimiterAndMD5Sum(resp.Body, cm.limiter, fileMd5)
	// forth: write to storage
	downloadMetadata, err := cm.writer.startWriter(ctx, reader, task, detectResult)
	if err != nil {
		logger.WithTaskID(task.TaskId).Errorf("failed to write for task: %v", err)
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}
	// back source length
	cm.metrics.cdnDownloadBytes.WithLabelValues().Add(float64(downloadMetadata.backSourceLength))

	//todo log
	// server.StatSeedFinish()

	sourceMD5 := reader.Md5()
	// fifth: handle CDN result
	success, err := cm.handleCDNResult(ctx, task, sourceMD5, downloadMetadata)
	if err != nil || !success {
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}
	return getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, sourceMD5, downloadMetadata.pieceMd5Sign,
		downloadMetadata.realSourceFileLength, downloadMetadata.realCdnFileLength), nil
}

// Delete the cdn meta with specified TaskId.
// It will also delete the files on the disk when the force equals true.
func (cm *Manager) Delete(ctx context.Context, TaskId string, force bool) error {
	if force {
		err := cm.cacheStore.DeleteTask(ctx, TaskId)
		if err != nil {
			return errors.Wrap(err, "failed to delete task files")
		}
	}
	err := cm.progressMgr.Clear(ctx, TaskId)
	if err != nil && !cdnerrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear progress")
	}
	return nil
}

// handleCDNResult
func (cm *Manager) handleCDNResult(ctx context.Context, task *types.SeedTask, sourceMd5 string,
	downloadMetadata *downloadMetadata) (bool, error) {
	logger.WithTaskID(task.TaskId).Debugf("handle cdn result, downloadMetaData: %+v", downloadMetadata)
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
		logger.WithTaskID(task.TaskId).Error(errorMsg)
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
	if err := cm.metaDataManager.updateStatusAndResult(ctx, task.TaskId, &storage.FileMetaData{
		Finish:        true,
		Success:       isSuccess,
		SourceRealMd5: sourceMd5,
		PieceMd5Sign:  pieceMd5Sign,
		CdnFileLength: cdnFileLength,
		SourceFileLen: sourceFileLen,
		TotalPieceCount: downloadMetadata.pieceTotalCount,
	}); err != nil {
		return false, errors.Wrap(err, "failed to update task status and result")
	}

	if !isSuccess {
		return false, errors.New(errorMsg)
	}

	logger.WithTaskID(task.TaskId).Infof("success to get task, downloadMetadata:%+v realMd5: %s", downloadMetadata, sourceMd5)

	return true, nil
}

func (cm *Manager) updateExpireInfo(ctx context.Context, TaskId string, expireInfo map[string]string) {
	if err := cm.metaDataManager.updateExpireInfo(ctx, TaskId, expireInfo); err != nil {
		logger.WithTaskID(TaskId).Errorf("failed to update expireInfo(%s): %v", expireInfo, err)
	}
	logger.WithTaskID(TaskId).Infof("success to update expireInfo(%s)", expireInfo)
}
