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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/gc"
	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
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

const StorageMode = storage.DiskStorageMode

var (
	_ gc.Executor     = (*diskStorageMgr)(nil)
	_ storage.Manager = (*diskStorageMgr)(nil)
)

func init() {
	storage.Register(StorageMode, newStorageManager)
}

func newStorageManager(cfg *storage.Config) (storage.Manager, error) {
	if len(cfg.DriverConfigs) != 1 {
		return nil, fmt.Errorf("disk storage manager should have only one disk driver, cfg's driver number is wrong config: %v", cfg)
	}
	diskDriver, ok := storedriver.Get(local.DiskDriverName)
	if !ok {
		return nil, fmt.Errorf("can not find disk driver for disk storage manager, config is %#v", cfg)
	}

	storageMgr := &diskStorageMgr{
		cfg:        cfg,
		diskDriver: diskDriver,
	}
	gc.Register("diskStorage", cfg.GCInitialDelay, cfg.GCInterval, storageMgr)
	return storageMgr, nil
}

type diskStorageMgr struct {
	cfg        *storage.Config
	diskDriver storedriver.Driver
	cleaner    *storage.Cleaner
	taskMgr    daemon.SeedTaskMgr
}

func (s *diskStorageMgr) getDefaultGcConfig() *storage.GCConfig {
	totalSpace, err := s.diskDriver.GetTotalSpace()
	if err != nil {
		logger.GcLogger.With("type", "disk").Errorf("get total space of disk: %v", err)
	}
	yongGcThreshold := 200 * unit.GB
	if totalSpace > 0 && totalSpace/4 < yongGcThreshold {
		yongGcThreshold = totalSpace / 4
	}
	return &storage.GCConfig{
		YoungGCThreshold:  yongGcThreshold,
		FullGCThreshold:   25 * unit.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}
}

func (s *diskStorageMgr) Initialize(taskMgr daemon.SeedTaskMgr) {
	s.taskMgr = taskMgr
	diskGcConfig := s.cfg.DriverConfigs[local.DiskDriverName].GCConfig
	if diskGcConfig == nil {
		diskGcConfig = s.getDefaultGcConfig()
		logger.GcLogger.With("type", "disk").Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	s.cleaner, _ = storage.NewStorageCleaner(diskGcConfig, s.diskDriver, s, taskMgr)
}

func (s *diskStorageMgr) AppendPieceMetaData(taskID string, pieceRecord *storage.PieceMetaRecord) error {
	return s.diskDriver.PutBytes(storage.GetAppendPieceMetaDataRaw(taskID), []byte(pieceRecord.String()+"\n"))
}

func (s *diskStorageMgr) ReadPieceMetaRecords(taskID string) ([]*storage.PieceMetaRecord, error) {
	readBytes, err := s.diskDriver.GetBytes(storage.GetPieceMetaDataRaw(taskID))
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

func (s *diskStorageMgr) GC() error {
	logger.GcLogger.With("type", "disk").Info("start the disk storage gc job")
	gcTaskIDs, err := s.cleaner.GC("disk", false)
	if err != nil {
		logger.GcLogger.With("type", "disk").Error("failed to get gcTaskIDs")
	}
	var realGCCount int
	for _, taskID := range gcTaskIDs {
		synclock.Lock(taskID, false)
		// try to ensure the taskID is not using again
		if _, err := s.taskMgr.Get(taskID); err == nil || !cdnerrors.IsDataNotFound(err) {
			if err != nil {
				logger.GcLogger.With("type", "disk").Errorf("failed to get taskID(%s): %v", taskID, err)
			}
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

func (s *diskStorageMgr) WriteDownloadFile(taskID string, offset int64, len int64, buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskID)
	raw.Offset = offset
	raw.Length = len
	return s.diskDriver.Put(raw, buf)
}

func (s *diskStorageMgr) ReadFileMetaData(taskID string) (*storage.FileMetaData, error) {
	bytes, err := s.diskDriver.GetBytes(storage.GetTaskMetaDataRaw(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "get metadata bytes")
	}

	metaData := &storage.FileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "unmarshal metadata bytes")
	}
	return metaData, nil
}

func (s *diskStorageMgr) WriteFileMetaData(taskID string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "marshal metadata")
	}
	return s.diskDriver.PutBytes(storage.GetTaskMetaDataRaw(taskID), data)
}

func (s *diskStorageMgr) WritePieceMetaRecords(taskID string, records []*storage.PieceMetaRecord) error {
	recordStrs := make([]string, 0, len(records))
	for i := range records {
		recordStrs = append(recordStrs, records[i].String())
	}
	pieceRaw := storage.GetPieceMetaDataRaw(taskID)
	pieceRaw.Trunc = true
	pieceRaw.TruncSize = 0
	return s.diskDriver.PutBytes(pieceRaw, []byte(strings.Join(recordStrs, "\n")))
}

func (s *diskStorageMgr) ReadPieceMetaBytes(taskID string) ([]byte, error) {
	return s.diskDriver.GetBytes(storage.GetPieceMetaDataRaw(taskID))
}

func (s *diskStorageMgr) ReadDownloadFile(taskID string) (io.ReadCloser, error) {
	return s.diskDriver.Get(storage.GetDownloadRaw(taskID))
}

func (s *diskStorageMgr) StatDownloadFile(taskID string) (*storedriver.StorageInfo, error) {
	return s.diskDriver.Stat(storage.GetDownloadRaw(taskID))
}

func (s *diskStorageMgr) CreateUploadLink(taskID string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(s.diskDriver.GetPath(storage.GetDownloadRaw(taskID)),
		s.diskDriver.GetPath(storage.GetUploadRaw(taskID))); err != nil {
		return err
	}
	return nil
}

func (s *diskStorageMgr) DeleteTask(taskID string) error {
	if err := s.diskDriver.Remove(storage.GetTaskMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetPieceMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	if err := s.diskDriver.Remove(storage.GetUploadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	// try to clean the parent bucket
	if err := s.diskDriver.Remove(storage.GetParentRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		logrus.Warnf("taskID:%s failed remove parent bucket:%v", taskID, err)
	}
	return nil
}

func (s *diskStorageMgr) ResetRepo(task *types.SeedTask) error {
	return s.DeleteTask(task.TaskID)
}
