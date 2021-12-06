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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/cdn/gc"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
)

const _disk = "disk"

var (
	_ gc.Executor     = (*diskStorageManager)(nil)
	_ storage.Manager = (*diskStorageManager)(nil)
	_ storage.Builder = (*diskStorageBuilder)(nil)
)

type diskStorageBuilder struct{}

func (b *diskStorageBuilder) Build(storageConfig storage.Config, taskManager task.Manager) (storage.Manager, error) {
	if len(storageConfig.DriverConfigs) != 1 {
		return nil, fmt.Errorf("disk storage manager should have only one disk driver, cfg's driver number is wrong. config: %v", storageConfig)
	}
	driverNames := make([]string, 0, len(storageConfig.DriverConfigs))
	for k := range storageConfig.DriverConfigs {
		driverNames = append(driverNames, k)
	}
	diskDriver, ok := storedriver.Get(driverNames[0])
	if !ok {
		return nil, fmt.Errorf("can not find disk driver for disk storage manager, config is %#v", storageConfig)
	}
	config := applyDefaults(diskDriver, storageConfig)
	storageManager := &diskStorageManager{
		config:      config,
		diskDriver:  diskDriver,
		taskManager: taskManager,
	}
	cleaner, err := storage.NewStorageCleaner(config.GCConfig, diskDriver, storageManager, taskManager)
	if err != nil {
		return nil, err
	}
	storageManager.diskCleaner = cleaner
	gc.Register("diskStorage", config.GCInitialDelay, config.GCInterval, storageManager)
	return storageManager, nil
}

func (*diskStorageBuilder) Name() string {
	return _disk
}

func init() {
	// TODO add support OS
	storage.Register(&diskStorageBuilder{})
}

type diskStorageManager struct {
	config      Config
	diskDriver  storedriver.Driver
	diskCleaner storage.Cleaner
	taskManager task.Manager
}

func (s *diskStorageManager) AppendPieceMetadata(taskID string, pieceRecord *storage.PieceMetaRecord) error {
	// TODO json
	return s.diskDriver.PutBytes(storage.GetAppendPieceMetadataRaw(taskID), []byte(pieceRecord.String()+"\n"))
}

func (s *diskStorageManager) ReadPieceMetaRecords(taskID string) ([]*storage.PieceMetaRecord, error) {
	readBytes, err := s.diskDriver.GetBytes(storage.GetPieceMetadataRaw(taskID))
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(readBytes)), "\n")
	var result = make([]*storage.PieceMetaRecord, 0, len(pieceMetaRecords))
	for _, pieceStr := range pieceMetaRecords {
		record, err := storage.ParsePieceMetaRecord(pieceStr)
		if err != nil {
			return nil, errors.Wrapf(err, "get piece meta record: %v", pieceStr)
		}
		result = append(result, record)
	}
	return result, nil
}

func (s *diskStorageManager) WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error {
	raw := storage.GetDownloadRaw(taskID)
	raw.Offset = offset
	raw.Length = len
	return s.diskDriver.Put(raw, data)
}

func (s *diskStorageManager) ReadFileMetadata(taskID string) (*storage.FileMetadata, error) {
	bytes, err := s.diskDriver.GetBytes(storage.GetTaskMetadataRaw(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "get metadata bytes")
	}

	metadata := &storage.FileMetadata{}
	if err := json.Unmarshal(bytes, metadata); err != nil {
		return nil, errors.Wrapf(err, "unmarshal metadata bytes")
	}
	return metadata, nil
}

func (s *diskStorageManager) WriteFileMetadata(taskID string, metadata *storage.FileMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "marshal metadata")
	}
	return s.diskDriver.PutBytes(storage.GetTaskMetadataRaw(taskID), data)
}

func (s *diskStorageManager) WritePieceMetaRecords(taskID string, records []*storage.PieceMetaRecord) error {
	recordStrs := make([]string, 0, len(records))
	for i := range records {
		recordStrs = append(recordStrs, records[i].String())
	}
	pieceRaw := storage.GetPieceMetadataRaw(taskID)
	pieceRaw.Trunc = true
	pieceRaw.TruncSize = 0
	return s.diskDriver.PutBytes(pieceRaw, []byte(strings.Join(recordStrs, "\n")))
}

func (s *diskStorageManager) ReadPieceMetaBytes(taskID string) ([]byte, error) {
	return s.diskDriver.GetBytes(storage.GetPieceMetadataRaw(taskID))
}

func (s *diskStorageManager) ReadDownloadFile(taskID string) (io.ReadCloser, error) {
	return s.diskDriver.Get(storage.GetDownloadRaw(taskID))
}

func (s *diskStorageManager) StatDownloadFile(taskID string) (*storedriver.StorageInfo, error) {
	storageInfo, err := s.diskDriver.Stat(storage.GetDownloadRaw(taskID))
	if err != nil && os.IsNotExist(err) {
		return nil, storage.ErrTaskNotPersisted
	}
	return storageInfo, err
}

func (s *diskStorageManager) ResetRepo(seedTask *task.SeedTask) error {
	if err := s.DeleteTask(seedTask.ID); err != nil {
		return errors.Errorf("delete task %s files: %v", seedTask.ID, err)
	}
	// create download file
	var downloadPath = s.diskDriver.GetPath(storage.GetDownloadRaw(seedTask.ID))
	if err := fileutils.MkdirAll(filepath.Dir(downloadPath)); err != nil {
		return err
	}
	if _, err := os.Create(downloadPath); err != nil {
		return err
	}
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(s.diskDriver.GetPath(storage.GetDownloadRaw(seedTask.ID)),
		s.diskDriver.GetPath(storage.GetUploadRaw(seedTask.ID))); err != nil {
		return err
	}
	return nil
}

func (s *diskStorageManager) DeleteTask(taskID string) error {
	if err := s.diskDriver.Remove(storage.GetTaskMetadataRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetPieceMetadataRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetUploadRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// try to clean the parent bucket
	if err := s.diskDriver.Remove(storage.GetParentRaw(taskID)); err != nil && !os.IsNotExist(err) {
		logrus.Warnf("taskID: %s failed remove parent bucket: %v", taskID, err)
	}
	return nil
}

func (s *diskStorageManager) TryFreeSpace(fileLength int64) (bool, error) {
	freeSpace, err := s.diskDriver.GetFreeSpace()
	if err != nil {
		return false, err
	}
	if freeSpace > 100*unit.GB && freeSpace.ToNumber() > fileLength {
		return true, nil
	}
	// TODO Optimize this code by iterating through the task list
	remainder := atomic.NewInt64(0)
	r := &storedriver.Raw{
		WalkFn: func(filePath string, info os.FileInfo, err error) error {
			if fileutils.IsRegular(filePath) {
				taskID := strings.Split(path.Base(filePath), ".")[0]
				seedTask, exist := s.taskManager.Exist(taskID)
				if exist {
					var totalLen int64 = 0
					if seedTask.CdnFileLength > 0 {
						totalLen = seedTask.CdnFileLength
					} else {
						totalLen = seedTask.SourceFileLength
					}
					if totalLen > 0 {
						remainder.Add(totalLen - info.Size())
					}
				}
			}
			return nil
		},
	}
	if err := s.diskDriver.Walk(r); err != nil {
		return false, err
	}

	enoughSpace := freeSpace.ToNumber()-remainder.Load() > (fileLength + int64(5*unit.GB))
	if !enoughSpace {
		if _, err := s.diskCleaner.GC("disk", true); err != nil {
			return false, err
		}

		remainder.Store(0)
		if err := s.diskDriver.Walk(r); err != nil {
			return false, err
		}
		freeSpace, err = s.diskDriver.GetFreeSpace()
		if err != nil {
			return false, err
		}
		enoughSpace = freeSpace.ToNumber()-remainder.Load() > (fileLength + int64(5*unit.GB))
	}
	if !enoughSpace {
		return false, nil
	}

	return true, nil
}

func (s *diskStorageManager) GC() error {
	logger.GcLogger.With("type", "disk").Info("start the disk storage gc job")
	gcTaskIDs, err := s.diskCleaner.GC("disk", false)
	if err != nil {
		logger.GcLogger.With("type", "disk").Error("failed to get gcTaskIDs")
	}
	var realGCCount int
	for _, taskID := range gcTaskIDs {
		synclock.Lock(taskID, false)
		// try to ensure the taskID is not using again
		if _, exist := s.taskManager.Exist(taskID); exist {
			synclock.UnLock(taskID, false)
			continue
		}
		realGCCount++
		if err := s.DeleteTask(taskID); err != nil {
			logger.GcLogger.With("type", "disk").Errorf("failed to delete disk files with taskID(%s): %v", taskID, err)
			synclock.UnLock(taskID, false)
			continue
		}
		synclock.UnLock(taskID, false)
	}
	logger.GcLogger.With("type", "disk").Infof("at most %d tasks can be cleaned up, actual gc %d tasks", len(gcTaskIDs), realGCCount)
	return nil
}
