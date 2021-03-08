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
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/store/disk"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
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

	var diskStorage *diskStorage = nil
	var _ storage.StorageMgr = diskStorage
}

type diskBuilder struct {
}

func (*diskBuilder) Build() (storage.StorageMgr, error) {
	diskStore, err := store.Get(disk.StorageDriver)
	if err != nil {
		return nil, err
	}
	storage := &diskStorage{
		diskStore: diskStore,
	}
	return storage, nil
}

func (*diskBuilder) Name() string {
	return name
}

type diskStorage struct {
	diskStore        *store.Store
	diskStoreCleaner *storage.Cleaner
	taskMgr          mgr.SeedTaskMgr
}

func (s *diskStorage) getDiskDefaultGcConfig() *storage.GcConfig {
	totalSpace, err := s.diskStore.GetTotalSpace(context.TODO())
	if err != nil {
		logger.GcLogger.Errorf("failed to get total space of disk: %v", err)
	}
	yongGcThreshold := 200 * fileutils.GB
	if totalSpace > 0 && totalSpace/4 < yongGcThreshold {
		yongGcThreshold = totalSpace / 4
	}
	return &storage.GcConfig{
		YoungGCThreshold:  yongGcThreshold,
		FullGCThreshold:   25 * fileutils.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}
}

func (s *diskStorage) InitializeCleaners() {
	s.diskStoreCleaner = &storage.Cleaner{
		Cfg: s.getDiskDefaultGcConfig(),
	}
}

func (s *diskStorage) AppendPieceMetaData(ctx context.Context, taskId string, pieceRecord *storage.PieceMetaRecord) error {
	data := getPieceMetaValue(pieceRecord)
	return s.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), []byte(data+"\n"))
}

func (s *diskStorage) ReadPieceMetaRecords(ctx context.Context, taskId string) ([]*storage.PieceMetaRecord, error) {
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

func (s *diskStorage) Gc(ctx context.Context) {
	panic("implement me")
}

func (s *diskStorage) SetTaskMgr(mgr mgr.SeedTaskMgr) {
	s.taskMgr = mgr
}

func (s *diskStorage) WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64, buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskId)
	raw.Offset = offset
	raw.Length = len
	return s.diskStore.Put(ctx, raw, buf)
}

func (s *diskStorage) ReadFileMetaData(ctx context.Context, taskId string) (*storage.FileMetaData, error) {
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

func (s *diskStorage) WriteFileMetaData(ctx context.Context, taskId string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return s.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskId), data)
}

func (s *diskStorage) AppendPieceMetaDataBytes(ctx context.Context, taskId string, bytes []byte) error {
	return s.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), bytes)
}

func (s *diskStorage) ReadPieceMetaBytes(ctx context.Context, taskId string) ([]byte, error) {
	return s.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
}

func (s *diskStorage) ReadDownloadFile(ctx context.Context, taskId string) (io.Reader, error) {
	return s.diskStore.Get(ctx, storage.GetDownloadRaw(taskId))
}

func (s *diskStorage) StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error) {
	return s.diskStore.Stat(ctx, storage.GetDownloadRaw(taskId))
}

func (s *diskStorage) CreateUploadLink(ctx context.Context, taskId string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(s.diskStore.GetPath(storage.GetDownloadRaw(taskId)),
		s.diskStore.GetPath(storage.GetUploadRaw(taskId))); err != nil {
		return err
	}
	return nil
}

func (s *diskStorage) DeleteTask(ctx context.Context, taskId string) error {
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

func (s *diskStorage) ResetRepo(ctx context.Context, task *types.SeedTask) error {
	return s.DeleteTask(ctx, task.TaskId)
}

func init() {
	storage.Register(&diskBuilder{})
}
