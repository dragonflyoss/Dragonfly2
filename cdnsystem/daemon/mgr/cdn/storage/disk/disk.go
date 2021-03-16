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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/store/disk"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/fsize"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
	"time"
)

const name = "disk"

func init() {
	var builder *diskBuilder = nil
	var _ storage.Builder = builder

	var diskStorage *diskStorageMgr = nil
	var _ storage.StorageMgr = diskStorage
}

type diskBuilder struct {
}

func (*diskBuilder) Build(cfg *config.Config) (storage.StorageMgr, error) {
	diskStore, err := store.Get(disk.StorageDriver)
	if err != nil {
		return nil, err
	}
	storage := &diskStorageMgr{
		diskStore: diskStore,
	}
	return storage, nil
}

func (*diskBuilder) Name() string {
	return name
}

type diskStorageMgr struct {
	diskStore        *store.Store
	diskStoreCleaner *storage.Cleaner
	taskMgr          mgr.SeedTaskMgr
}

func (s *diskStorageMgr) getDiskDefaultGcConfig() *store.GcConfig {
	totalSpace, err := s.diskStore.GetTotalSpace(context.TODO())
	if err != nil {
		logger.GcLogger.Errorf("failed to get total space of disk: %v", err)
	}
	yongGcThreshold := 200 * fsize.GB
	if totalSpace > 0 && totalSpace/4 < yongGcThreshold {
		yongGcThreshold = totalSpace / 4
	}
	return &store.GcConfig{
		YoungGCThreshold:  yongGcThreshold,
		FullGCThreshold:   25 * fsize.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}
}

func (s *diskStorageMgr) InitializeCleaners() {
	diskGcConfig := s.diskStore.GetGcConfig(context.TODO())
	if diskGcConfig == nil {
		diskGcConfig = s.getDiskDefaultGcConfig()
		logger.GcLogger.Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	s.diskStoreCleaner = &storage.Cleaner{
		Cfg:        diskGcConfig,
		Store:      s.diskStore,
		StorageMgr: s,
		TaskMgr:    s.taskMgr,
	}
}

func (s *diskStorageMgr) AppendPieceMetaData(ctx context.Context, taskId string, pieceRecord *storage.PieceMetaRecord) error {
	data := getPieceMetaValue(pieceRecord)
	return s.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), []byte(data+"\n"))
}

func (s *diskStorageMgr) ReadPieceMetaRecords(ctx context.Context, taskId string) ([]*storage.PieceMetaRecord, error) {
	bytes, err := s.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	var result = make([]*storage.PieceMetaRecord, 0)
	for _, pieceStr := range pieceMetaRecords {
		record, err := parsePieceMetaRecord(pieceStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get piece meta record:%v", pieceStr)
		}
		result = append(result, record)
	}
	return result, nil
}

func (s *diskStorageMgr) GC(ctx context.Context) error {
	gcTaskIDs, err := s.diskStoreCleaner.Gc(ctx, false)
	if err != nil {
		logger.GcLogger.Error("gc disk: failed to get gcTaskIds")
	}
	for _, taskID := range gcTaskIDs {
		util.GetLock(taskID, false)
		// try to ensure the taskID is not using again
		if _, err := s.taskMgr.Get(ctx, taskID); err == nil || !cdnerrors.IsDataNotFound(err) {
			if err != nil {
				logger.GcLogger.Errorf("gc disk: failed to get taskID(%s): %v", taskID, err)
			}
			util.ReleaseLock(taskID, false)
			continue
		}
		if err := s.DeleteTask(ctx, taskID); err != nil {
			logger.GcLogger.Errorf("gc disk: failed to delete disk files with taskID(%s): %v", taskID, err)
			util.ReleaseLock(taskID, false)
			continue
		}
		util.ReleaseLock(taskID, false)
	}
	return nil
}

func (s *diskStorageMgr) SetTaskMgr(mgr mgr.SeedTaskMgr) {
	s.taskMgr = mgr
}

func (s *diskStorageMgr) WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64, buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskId)
	raw.Offset = offset
	raw.Length = len
	return s.diskStore.Put(ctx, raw, buf)
}

func (s *diskStorageMgr) ReadFileMetaData(ctx context.Context, taskId string) (*storage.FileMetaData, error) {
	bytes, err := s.diskStore.GetBytes(ctx, storage.GetTaskMetaDataRaw(taskId))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &storage.FileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	return metaData, nil
}

func (s *diskStorageMgr) WriteFileMetaData(ctx context.Context, taskId string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return s.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskId), data)
}

func (s *diskStorageMgr) AppendPieceMetaDataBytes(ctx context.Context, taskId string, bytes []byte) error {
	return s.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), bytes)
}

func (s *diskStorageMgr) ReadPieceMetaBytes(ctx context.Context, taskId string) ([]byte, error) {
	return s.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
}

func (s *diskStorageMgr) ReadDownloadFile(ctx context.Context, taskId string) (io.Reader, error) {
	return s.diskStore.Get(ctx, storage.GetDownloadRaw(taskId))
}

func (s *diskStorageMgr) StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error) {
	return s.diskStore.Stat(ctx, storage.GetDownloadRaw(taskId))
}

func (s *diskStorageMgr) CreateUploadLink(ctx context.Context, taskId string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(s.diskStore.GetPath(storage.GetDownloadRaw(taskId)),
		s.diskStore.GetPath(storage.GetUploadRaw(taskId))); err != nil {
		return err
	}
	return nil
}

func (s *diskStorageMgr) DeleteTask(ctx context.Context, taskId string) error {
	if err := s.diskStore.Remove(ctx, storage.GetTaskMetaDataRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		errors.Cause(err)
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetPieceMetaDataRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetDownloadRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		return err
	}
	if err := s.diskStore.Remove(ctx, storage.GetUploadRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		return err
	}
	// try to clean the parent bucket
	if err := s.diskStore.Remove(ctx, storage.GetParentRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		logrus.Warnf("taskID:%s failed remove parent bucket:%v", taskId, err)
	}
	return nil
}

func (s *diskStorageMgr) ResetRepo(ctx context.Context, task *types.SeedTask) error {
	return s.DeleteTask(ctx, task.TaskId)
}

func init() {
	storage.Register(&diskBuilder{})
}
