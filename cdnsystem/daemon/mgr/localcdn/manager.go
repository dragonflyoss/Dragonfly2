package localcdn

import (
	"context"
	"crypto/md5"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"path"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/limitreader"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/metricsutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/rate/ratelimiter"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
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
	cfg                  *config.Config
	cacheStore           *store.Store
	limiter              *ratelimiter.RateLimiter
	cdnLocker            *util.LockerPool
	metaDataManager      *fileMetaDataManager
	pieceMetaDataManager *pieceMetaDataManager
	cdnReporter          *reporter
	detector             *cacheDetector
	resourceClient       source.ResourceClient
	writer               *cacheWriter
	metrics              *metrics
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore *store.Store, resourceClient source.ResourceClient,
	rateLimiter *ratelimiter.RateLimiter, register prometheus.Registerer) (mgr.CDNMgr, error) {
	return newManager(cfg, cacheStore, resourceClient, rateLimiter, register)
}

func newManager(cfg *config.Config, cacheStore *store.Store,
	resourceClient source.ResourceClient, rateLimiter *ratelimiter.RateLimiter, register prometheus.Registerer) (*Manager, error) {
	metaDataManager := newFileMetaDataManager(cacheStore)
	pieceMetaDataManager := newPieceMetaDataMgr(cacheStore)
	cdnReporter := newReporter(pieceMetaDataManager)
	return &Manager{
		cfg:                  cfg,
		cacheStore:           cacheStore,
		limiter:              rateLimiter,
		cdnLocker:            util.NewLockerPool(),
		metaDataManager:      metaDataManager,
		pieceMetaDataManager: pieceMetaDataManager,
		cdnReporter:          cdnReporter,
		detector:             newCacheDetector(cacheStore, metaDataManager, pieceMetaDataManager, resourceClient),
		resourceClient:       resourceClient,
		writer:               newCacheWriter(cacheStore, cdnReporter),
		metrics:              newMetrics(register),
	}, nil
}

// TriggerCDN will trigger CDN to download the file from sourceUrl.
func (cm *Manager) TriggerCDN(ctx context.Context, task *types.SeedTaskInfo) (*types.SeedTaskInfo, error) {
	// obtain taskID write lock
	cm.cdnLocker.GetLock(task.TaskID, false)
	defer cm.cdnLocker.ReleaseLock(task.TaskID, false)
	// first: detect Cache
	detectResult, err := cm.detector.detectCache(ctx, task)
	if err != nil {
		logrus.Errorf("taskId: %s failed to detect cache err: %v", task.TaskID, err)
	}
	// second: report detect result
	updateTaskInfo, err := cm.cdnReporter.reportCache(task.TaskID, detectResult)
	if err != nil {
		logrus.Errorf("taskId: %s failed to report cache err: %v", task.TaskID, err)
	}

	if detectResult.breakNum == -1 {
		logrus.Infof("taskId: %s cache full hit on local", task.TaskID)
		cm.metrics.cdnCacheHitCount.WithLabelValues().Inc()
		return updateTaskInfo, nil
	}
	// 如果没有全部命中
	if detectResult.fileMd5 == nil {
		detectResult.fileMd5 = md5.New()
	}
	// third: start to download the source file
	resp, err := cm.download(ctx, task, detectResult)
	cm.metrics.cdnDownloadCount.WithLabelValues().Inc()
	if err != nil {
		cm.metrics.cdnDownloadFailCount.WithLabelValues().Inc()
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}
	defer resp.Body.Close()

	cm.updateExpireInfo(ctx, task.TaskID, resp.ExpireInfo)
	reader := limitreader.NewLimitReaderWithLimiterAndMD5Sum(resp.Body, cm.limiter, detectResult.fileMd5)
	// forth: write to storage
	downloadMetadata, err := cm.writer.startWriter(ctx, reader, task, detectResult)
	if err != nil {
		logrus.Errorf("failed to write for task %s: %v", task.TaskID, err)
		return getUpdateTaskInfoWithStatusOnly(types.TaskInfoCdnStatusFAILED), err
	}
	cm.metrics.cdnDownloadBytes.WithLabelValues().Add(float64(downloadMetadata.realSourceFileLength))

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
func (cm *Manager) GetHTTPPath(ctx context.Context, task *types.SeedTaskInfo) (string, error) {
	raw := getDownloadRawFunc(task.TaskID)
	return path.Join("/", raw.Bucket, raw.Key), nil
}

// GetStatus gets the status of the file.
func (cm *Manager) GetStatus(ctx context.Context, taskID string) (cdnStatus string, err error) {
	return types.TaskInfoCdnStatusSUCCESS, nil
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
func (cm *Manager) Delete(ctx context.Context, taskID string, force bool) error {
	if !force {
		return cm.pieceMetaDataManager.removePieceMetaRecordsByTaskID(taskID)
	}

	return deleteTaskFiles(ctx, cm.cacheStore, taskID)
}

func (cm *Manager) handleCDNResult(ctx context.Context, task *types.SeedTaskInfo, sourceMd5 string, downloadMetadata *downloadMetadata) (bool, error) {
	sourceFileLength := task.SourceFileLength
	realDownloadedSourceFileLength := downloadMetadata.realSourceFileLength
	var isSuccess = true
	// check md5
	if !stringutils.IsEmptyStr(task.RequestMd5) && task.RequestMd5 != sourceMd5 {
		logrus.Errorf("taskId:%s url:%s file md5 not match expected:%s real:%s", task.TaskID, task.Url, task.RequestMd5, sourceMd5)
		isSuccess = false
	}
	// check source length
	if isSuccess && sourceFileLength >= 0 && sourceFileLength != realDownloadedSourceFileLength {
		logrus.Errorf("taskId:%s url:%s file length not match expected:%d real:%d", task.TaskID, task.Url, sourceFileLength, realDownloadedSourceFileLength)
		isSuccess = false
	}

	if !isSuccess {
		realDownloadedSourceFileLength = 0
	}
	if err := cm.metaDataManager.updateStatusAndResult(ctx, task.TaskID, &fileMetaData{
		Finish:        true,
		Success:       isSuccess,
		SourceMd5:     sourceMd5,
		CdnFileLength: downloadMetadata.realCdnFileLength,
	}); err != nil {
		return false, err
	}

	if !isSuccess {
		return false, nil
	}

	logrus.Infof("success to get taskID: %s fileLength: %d realMd5: %s", task.TaskID, downloadMetadata.realCdnFileLength, sourceMd5)

	pieceMD5s, err := cm.pieceMetaDataManager.getPieceMetaRecordsByTaskID(task.TaskID)
	if err != nil {
		return false, err
	}

	if err := cm.pieceMetaDataManager.writePieceMetaRecords(ctx, task.TaskID, sourceMd5, pieceMD5s); err != nil {
		return false, err
	}
	return true, nil
}

func (cm *Manager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) {
	if err := cm.metaDataManager.updateExpireInfo(ctx, taskID, expireInfo); err != nil {
		logrus.Errorf("taskID: %s failed to update expireInfo(%s): %v", taskID, expireInfo, err)
	}
	logrus.Infof("taskID: %s success to update expireInfo(%s)", expireInfo, taskID)
}
