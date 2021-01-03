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
	"crypto/md5"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/progress"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate/limitreader"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate/ratelimiter"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/metricsutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/prometheus/client_golang/prometheus"
	"path"
)

var _ mgr.CDNMgr = &Manager{}

func init() {
	mgr.Register(config.CDNPatternLocal, NewManager)
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
	cacheStore      *store.Store
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

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore *store.Store, resourceClient source.ResourceClient, register prometheus.Registerer) (mgr.CDNMgr, error) {
	return newManager(cfg, cacheStore, resourceClient, register)
}

func newManager(cfg *config.Config, cacheStore *store.Store, resourceClient source.ResourceClient, register prometheus.Registerer) (*Manager, error) {
	rateLimiter := ratelimiter.NewRateLimiter(ratelimiter.TransRate(int64(cfg.MaxBandwidth-cfg.SystemReservedBandwidth)), 2)
	metaDataManager := newFileMetaDataManager(cacheStore)
	publisher := progress.NewManager()
	cdnReporter := newReporter(publisher)
	return &Manager{
		cfg:             cfg,
		cacheStore:      cacheStore,
		limiter:         rateLimiter,
		cdnLocker:       util.NewLockerPool(),
		metaDataManager: metaDataManager,
		cdnReporter:     cdnReporter,
		progressMgr:     publisher,
		detector:        newCacheDetector(cacheStore, metaDataManager, resourceClient),
		resourceClient:  resourceClient,
		writer:          newCacheWriter(cacheStore, cdnReporter, metaDataManager),
		metrics:         newMetrics(register),
	}, nil
}

// TriggerCDN will trigger CDN to download the file from sourceUrl.
func (cm *Manager) TriggerCDN(ctx context.Context, task *types.SeedTask) (*types.SeedTask, error) {
	defer func() {
		//cm.cdnReporter.reportTask(task.TaskID, )
	}()
	// obtain taskID write lock
	cm.cdnLocker.GetLock(task.TaskID, false)
	defer cm.cdnLocker.ReleaseLock(task.TaskID, false)
	// first: detect Cache
	detectResult, err := cm.detector.detectCache(ctx, task)
	if err != nil {
		logger.Named(task.TaskID).Errorf("failed to detect cache:%v", err)
		detectResult = &cacheResult{} // reset cacheResult
	}
	// second: report detect result
	err = cm.cdnReporter.reportCache(task.TaskID, detectResult)
	if err != nil {
		logger.Named(task.TaskID).Errorf("failed to report cache, reset detectResult:%v", err)
		// todo reset
		detectResult.breakNum = 0
		detectResult.fileMd5.Reset()
	}
	// full cache
	if detectResult.breakNum == -1 {
		logger.Named(task.TaskID).Infof("cache full hit on local", task.TaskID)
		cm.metrics.cdnCacheHitCount.WithLabelValues().Inc()
		return getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, detectResult.fileMetaData.SourceMd5, detectResult.fileMetaData.CdnFileLength), nil
	}

	if detectResult.fileMd5 == nil {
		detectResult.fileMd5 = md5.New()
	}
	// third: start to download the source file
	resp, err := cm.download(task, detectResult)
	cm.metrics.cdnDownloadCount.WithLabelValues().Inc()
	// download fail
	if err != nil {
		cm.metrics.cdnDownloadFailCount.WithLabelValues().Inc()
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusSOURCEERROR), err
	}
	defer resp.Body.Close()
	// update Expire info
	cm.updateExpireInfo(ctx, task.TaskID, resp.ExpireInfo)
	reader := limitreader.NewLimitReaderWithLimiterAndMD5Sum(resp.Body, cm.limiter, detectResult.fileMd5)
	// forth: write to storage
	downloadMetadata, err := cm.writer.startWriter(ctx, reader, task, detectResult)
	if err != nil {
		logger.Named(task.TaskID).Errorf("failed to write for task: %v", err)
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}
	cm.metrics.cdnDownloadBytes.WithLabelValues().Add(float64(downloadMetadata.backSourceLength))

	sourceMD5 := reader.Md5()
	// fifth: handle CDN result
	success, err := cm.handleCDNResult(ctx, task, sourceMD5, downloadMetadata)
	if err != nil || !success {
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}

	return getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, sourceMD5, downloadMetadata.realCdnFileLength), nil
}

// GetHTTPPath returns the http download path of taskID.
// The returned path joined the DownloadRaw.Bucket and DownloadRaw.Key.
func (cm *Manager) GetHTTPPath(ctx context.Context, task *types.SeedTask) (string, error) {
	raw := getDownloadRawFunc(task.TaskID)
	return path.Join("/", raw.Bucket, raw.Key), nil
}

// CheckFile checks the file whether exists.
func (cm *Manager) CheckFileExist(ctx context.Context, taskID string) bool {
	if _, err := cm.cacheStore.Stat(ctx, getDownloadRaw(taskID)); err != nil {
		return false
	}
	return true
}

// Delete the cdn meta with specified taskID.
// It will also delete the files on the disk when the force equals true.
func (cm *Manager) Delete(ctx context.Context, taskID string) error {
	return deleteTaskFiles(ctx, cm.cacheStore, taskID)
}

func (cm *Manager) WatchSeedTask(taskID string, taskMgr mgr.SeedTaskMgr) (<-chan *types.SeedPiece, error) {
	return cm.progressMgr.WatchSeedProgress(taskID, taskMgr)
}

// handleCDNResult
func (cm *Manager) handleCDNResult(ctx context.Context, task *types.SeedTask, sourceMd5 string, downloadMetadata *downloadMetadata) (bool, error) {
	var isSuccess = true
	// check md5
	if !stringutils.IsEmptyStr(task.RequestMd5) && task.RequestMd5 != sourceMd5 {
		logger.Named(task.TaskID).Errorf("file md5 not match expected:%s real:%s", task.RequestMd5, sourceMd5)
		isSuccess = false
	}
	// check source length
	if isSuccess && task.SourceFileLength >= 0 && task.SourceFileLength != downloadMetadata.realSourceFileLength {
		logger.Named(task.TaskID).Errorf("file length not match expected:%d real:%d", task.SourceFileLength, downloadMetadata.realSourceFileLength)
		isSuccess = false
	}
	if isSuccess && task.PieceTotal > 0 && downloadMetadata.pieceTotalCount != task.PieceTotal {
		logger.Named(task.TaskID).Errorf("task total piece count not match expected:%d real:%d", task.PieceTotal, downloadMetadata.pieceTotalCount)
		isSuccess = false
	}
	sourceFileLen := task.SourceFileLength
	if isSuccess && task.SourceFileLength <= 0 {
		sourceFileLen = downloadMetadata.realSourceFileLength
	}
	cdnFileLength := downloadMetadata.realCdnFileLength
	// if validate fail
	if !isSuccess {
		cdnFileLength = 0
	}
	if err := cm.metaDataManager.updateStatusAndResult(ctx, task.TaskID, &fileMetaData{
		Finish:        true,
		Success:       isSuccess,
		SourceMd5:     sourceMd5,
		CdnFileLength: cdnFileLength,
		SourceFileLen: sourceFileLen,
	}); err != nil {
		return false, err
	}

	if !isSuccess {
		return false, nil
	}

	logger.Named(task.TaskID).Infof("success to get task, downloadMetadata:%+v realMd5: %s", downloadMetadata, sourceMd5)

	if err := cm.metaDataManager.appendPieceMetaIntegrityData(ctx, task.TaskID, sourceMd5); err != nil {
		return false, err
	}
	return true, nil
}

func (cm *Manager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) {
	if err := cm.metaDataManager.updateExpireInfo(ctx, taskID, expireInfo); err != nil {
		logger.Named(taskID).Errorf("failed to update expireInfo(%s): %v", expireInfo, err)
	}
	logger.Named(taskID).Infof("success to update expireInfo(%s)", expireInfo)
}
