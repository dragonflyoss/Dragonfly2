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
	"crypto/md5"
	"encoding/json"
	"time"

	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"context"
	"fmt"

	"d7y.io/dragonfly/v2/cdnsystem/supervisor"
	_ "d7y.io/dragonfly/v2/cdnsystem/supervisor/cdn/storage/disk"   // To register diskStorage
	_ "d7y.io/dragonfly/v2/cdnsystem/supervisor/cdn/storage/hybrid" // To register hybridStorage
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"

	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/limitreader"
	"d7y.io/dragonfly/v2/pkg/ratelimiter/ratelimiter"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// Ensure that Manager implements the CDNMgr interface
var _ supervisor.CDNMgr = (*Manager)(nil)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("cdn-server")
}

// Manager is an implementation of the interface of CDNMgr.
type Manager struct {
	cfg              *config.Config
	cacheStore       storage.Manager
	limiter          *ratelimiter.RateLimiter
	cdnLocker        *synclock.LockerPool
	cacheDataManager *cacheDataManager
	progressMgr      supervisor.SeedProgressMgr
	cdnReporter      *reporter
	detector         *cacheDetector
	writer           *cacheWriter
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, cacheStore storage.Manager, progressMgr supervisor.SeedProgressMgr) (supervisor.CDNMgr, error) {
	return newManager(cfg, cacheStore, progressMgr)
}

func newManager(cfg *config.Config, cacheStore storage.Manager, progressMgr supervisor.SeedProgressMgr) (*Manager, error) {
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
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanTriggerCDN)
	defer span.End()
	tempTask := *task
	seedTask = &tempTask
	// obtain taskId write lock
	cm.cdnLocker.Lock(task.TaskID, false)
	defer cm.cdnLocker.UnLock(task.TaskID, false)

	var fileDigest = md5.New()
	var digestType = digestutils.Md5Hash.String()
	if !stringutils.IsBlank(task.RequestDigest) {
		requestDigest := digestutils.Parse(task.RequestDigest)
		digestType = requestDigest[0]
		fileDigest = digestutils.CreateHash(digestType)
	}
	// first: detect Cache
	detectResult, err := cm.detector.detectCache(ctx, task, fileDigest)
	if err != nil {
		seedTask.UpdateStatus(types.TaskInfoCdnStatusFailed)
		return seedTask, errors.Wrapf(err, "failed to detect cache")
	}
	resultBytes, _ := json.Marshal(detectResult)
	span.SetAttributes(config.AttributeCacheResult.String(string(resultBytes)))
	logger.WithTaskID(task.TaskID).Debugf("detects cache result: %+v", detectResult)
	// second: report detect result
	err = cm.cdnReporter.reportCache(ctx, task.TaskID, detectResult)
	if err != nil {
		logger.WithTaskID(task.TaskID).Errorf("failed to report cache, reset detectResult: %v", err)
	}
	// full cache
	if detectResult.breakPoint == -1 {
		logger.WithTaskID(task.TaskID).Infof("cache full hit on local")
		seedTask.UpdateTaskInfo(types.TaskInfoCdnStatusSuccess, detectResult.fileMetaData.SourceRealDigest, detectResult.fileMetaData.PieceMd5Sign,
			detectResult.fileMetaData.SourceFileLen, detectResult.fileMetaData.CdnFileLength)
		return seedTask, nil
	}
	server.StatSeedStart(task.TaskID, task.URL)
	start := time.Now()
	// third: start to download the source file
	var downloadSpan trace.Span
	ctx, downloadSpan = tracer.Start(ctx, config.SpanDownloadSource)
	downloadSpan.End()
	body, err := cm.download(ctx, task, detectResult)
	// download fail
	if err != nil {
		downloadSpan.RecordError(err)
		server.StatSeedFinish(task.TaskID, task.URL, false, err, start.Nanosecond(), time.Now().Nanosecond(), 0, 0)
		seedTask.UpdateStatus(types.TaskInfoCdnStatusSourceError)
		return seedTask, err
	}
	defer body.Close()
	reader := limitreader.NewLimitReaderWithLimiterAndDigest(body, cm.limiter, fileDigest, digestutils.Algorithms[digestType])

	// forth: write to storage
	downloadMetadata, err := cm.writer.startWriter(ctx, reader, task, detectResult)
	if err != nil {
		server.StatSeedFinish(task.TaskID, task.URL, false, err, start.Nanosecond(), time.Now().Nanosecond(), downloadMetadata.backSourceLength,
			downloadMetadata.realSourceFileLength)
		logger.WithTaskID(task.TaskID).Errorf("failed to write for task: %v", err)
		seedTask.UpdateStatus(types.TaskInfoCdnStatusFailed)
		return seedTask, err
	}
	server.StatSeedFinish(task.TaskID, task.URL, true, nil, start.Nanosecond(), time.Now().Nanosecond(), downloadMetadata.backSourceLength,
		downloadMetadata.realSourceFileLength)
	sourceDigest := reader.Digest()
	// fifth: handle CDN result
	success, err := cm.handleCDNResult(task, sourceDigest, downloadMetadata)
	if err != nil || !success {
		seedTask.UpdateStatus(types.TaskInfoCdnStatusFailed)
		return seedTask, err
	}
	seedTask.UpdateTaskInfo(types.TaskInfoCdnStatusSuccess, sourceDigest, downloadMetadata.pieceMd5Sign,
		downloadMetadata.realSourceFileLength, downloadMetadata.realCdnFileLength)
	return seedTask, nil
}

func (cm *Manager) Delete(taskID string) error {
	err := cm.cacheStore.DeleteTask(taskID)
	if err != nil {
		return errors.Wrap(err, "failed to delete task files")
	}
	return nil
}

func (cm *Manager) handleCDNResult(task *types.SeedTask, sourceDigest string, downloadMetadata *downloadMetadata) (bool, error) {
	logger.WithTaskID(task.TaskID).Debugf("handle cdn result, downloadMetaData: %+v", downloadMetadata)
	var isSuccess = true
	var errorMsg string
	// check md5
	if !stringutils.IsBlank(task.RequestDigest) && task.RequestDigest != sourceDigest {
		errorMsg = fmt.Sprintf("file digest not match expected: %s real: %s", task.RequestDigest, sourceDigest)
		isSuccess = false
	}
	// check source length
	if isSuccess && task.SourceFileLength >= 0 && task.SourceFileLength != downloadMetadata.realSourceFileLength {
		errorMsg = fmt.Sprintf("file length not match expected: %d real: %d", task.SourceFileLength, downloadMetadata.realSourceFileLength)
		isSuccess = false
	}
	if isSuccess && task.PieceTotal > 0 && downloadMetadata.pieceTotalCount != task.PieceTotal {
		errorMsg = fmt.Sprintf("task total piece count not match expected: %d real: %d", task.PieceTotal, downloadMetadata.pieceTotalCount)
		isSuccess = false
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
		Finish:           true,
		Success:          isSuccess,
		SourceRealDigest: sourceDigest,
		PieceMd5Sign:     pieceMd5Sign,
		CdnFileLength:    cdnFileLength,
		SourceFileLen:    sourceFileLen,
		TotalPieceCount:  downloadMetadata.pieceTotalCount,
	}); err != nil {
		return false, errors.Wrap(err, "failed to update task status and result")
	}

	if !isSuccess {
		return false, errors.New(errorMsg)
	}

	logger.WithTaskID(task.TaskID).Infof("success to get task, downloadMetadata: %+v realDigest: %s", downloadMetadata, sourceDigest)

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
