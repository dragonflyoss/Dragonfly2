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

package disk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/gc"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver/local"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const name = "disk"

func init() {
	var builder *diskBuilder = nil
	var _ storage.Builder = builder

	var diskStorage *diskStorageMgr = nil
	var _ storage.Manager = diskStorage
	var _ gc.Executor = diskStorage
}

type diskBuilder struct {
}

func (*diskBuilder) Build(cfg *config.Config) (storage.Manager, error) {
	diskStore, err := storedriver.Get(local.StorageDriver)
	if err != nil {
		return nil, err
	}
	storageMgr := &diskStorageMgr{
		diskStore: diskStore,
	}
	gc.Register("diskStorage", cfg.GCInitialDelay, cfg.GCStorageInterval, storageMgr)
	return storageMgr, nil
}

func (*diskBuilder) Name() string {
	return name
}

type diskStorageMgr struct {
	diskStore        *storedriver.Store
	diskStoreCleaner *storage.Cleaner
	taskMgr          mgr.SeedTaskMgr
}

func (s *diskStorageMgr) getDiskDefaultGcConfig() *storedriver.GcConfig {
	totalSpace, err := s.diskStore.GetTotalSpace(context.TODO())
	if err != nil {
		logger.GcLogger.With("type", "disk").Errorf("get total space of disk: %v", err)
	}
	yongGcThreshold := 200 * unit.GB
	if totalSpace > 0 && totalSpace/4 < yongGcThreshold {
		yongGcThreshold = totalSpace / 4
	}
	return &storedriver.GcConfig{
		YoungGCThreshold:  yongGcThreshold,
		FullGCThreshold:   25 * unit.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}
}

func (s *diskStorageMgr) InitializeCleaners() {
	diskGcConfig := s.diskStore.GetGcConfig(context.TODO())
	if diskGcConfig == nil {
		diskGcConfig = s.getDiskDefaultGcConfig()
		logger.GcLogger.With("type", "disk").Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	s.diskStoreCleaner = &storage.Cleaner{
		Cfg:        diskGcConfig,
		Store:      s.diskStore,
		StorageMgr: s,
		TaskMgr:    s.taskMgr,
	}
}

func (s *diskStorageMgr) AppendPieceMetaData(ctx context.Context, taskID string, pieceRecord *storage.PieceMetaRecord) error {
	return s.diskStore.PutBytes(ctx, storage.GetAppendPieceMetaDataRaw(taskID), []byte(pieceRecord.String()+"\n"))
}

func (s *diskStorageMgr) ReadPieceMetaRecords(ctx context.Context, taskID string) ([]*storage.PieceMetaRecord, error) {
	bytes, err := s.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskID))
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	var result = make([]*storage.PieceMetaRecord, 0)
	for _, pieceStr := range pieceMetaRecords {
		record, err := storage.ParsePieceMetaRecord(pieceStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get piece meta record: %v", pieceStr)
		}
		result = append(result, record)
	}
	return result, nil
}

func (s *diskStorageMgr) GC(ctx context.Context) error {
	logger.GcLogger.With("type", "disk").Info("start the disk storage gc job")
	gcTaskIDs, err := s.diskStoreCleaner.Gc(ctx, "disk", false)
	if err != nil {
		logger.GcLogger.With("type", "disk").Error("failed to get gcTaskIDs")
	}
	var realGCCount int
	for _, taskID := range gcTaskIDs {
		synclock.Lock(taskID, false)
		// try to ensure the taskID is not using again
		if _, err := s.taskMgr.Get(ctx, taskID); err == nil || !cdnerrors.IsDataNotFound(err) {
			if err != nil {
				logger.GcLogger.With("type", "disk").Errorf("failed to get taskID(%s): %v", taskID, err)
			}
			synclock.UnLock(taskID, false)
			continue
		}
		realGCCount++
		if err := s.DeleteTask(ctx, taskID); err != nil {
			logger.GcLogger.With("type", "disk").Errorf("failed to delete disk files with taskID(%s): %v", taskID, err)
			synclock.UnLock(taskID, false)
			continue
		}
		synclock.UnLock(taskID, false)
	}
	logger.GcLogger.With("type", "disk").Infof("at most %d tasks can be cleaned up, actual gc %d tasks", len(gcTaskIDs), realGCCount)
	return nil
}

func (s *diskStorageMgr) SetTaskMgr(mgr mgr.SeedTaskMgr) {
	s.taskMgr = mgr
}

func (s *diskStorageMgr) WriteDownloadFile(ctx context.Context, taskID string, offset int64, len int64, buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskID)
	raw.Offset = offset
	raw.Length = len
	return s.diskStore.Put(ctx, raw, buf)
}

func (s *diskStorageMgr) ReadFileMetaData(ctx context.Context, taskID string) (*storage.FileMetaData, error) {
	bytes, err := s.diskStore.GetBytes(ctx, storage.GetTaskMetaDataRaw(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &storage.FileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	return metaData, nil
}

func (s *diskStorageMgr) WriteFileMetaData(ctx context.Context, taskID string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return s.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskID), data)
}

func (s *diskStorageMgr) WritePieceMetaRecords(ctx context.Context, taskID string, records []*storage.PieceMetaRecord) error {
	recordStrs := make([]string, 0, len(records))
	for i := range records {
		recordStrs = append(recordStrs, records[i].String())
	}
	pieceRaw := storage.GetPieceMetaDataRaw(taskID)
	pieceRaw.Trunc = true
	pieceRaw.TruncSize = 0
	return s.diskStore.PutBytes(ctx, pieceRaw, []byte(strings.Join(recordStrs, "\n")))
}

func (s *diskStorageMgr) ReadPieceMetaBytes(ctx context.Context, taskID string) ([]byte, error) {
	return s.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskID))
}

func (s *diskStorageMgr) ReadDownloadFile(ctx context.Context, taskID string) (io.ReadCloser, error) {
	return s.diskStore.Get(ctx, storage.GetDownloadRaw(taskID))
}

func (s *diskStorageMgr) StatDownloadFile(ctx context.Context, taskID string) (*storedriver.StorageInfo, error) {
	return s.diskStore.Stat(ctx, storage.GetDownloadRaw(taskID))
}

func (s *diskStorageMgr) CreateUploadLink(ctx context.Context, taskID string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(s.diskStore.GetPath(storage.GetDownloadRaw(taskID)),
		s.diskStore.GetPath(storage.GetUploadRaw(taskID))); err != nil {
		return err
	}
	return nil
}

func (s *diskStorageMgr) DeleteTask(ctx context.Context, taskID string) error {
	if err := s.diskStore.Remove(ctx, storage.GetTaskMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		errors.Cause(err)
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetPieceMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetDownloadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetUploadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	// try to clean the parent bucket
	if err := s.diskStore.Remove(ctx, storage.GetParentRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		logrus.Warnf("taskID:%s failed remove parent bucket:%v", taskID, err)
	}
	return nil
}

func (s *diskStorageMgr) ResetRepo(ctx context.Context, task *types.SeedTask) error {
	return s.DeleteTask(ctx, task.TaskID)
}

func init() {
	storage.Register(&diskBuilder{})
}
