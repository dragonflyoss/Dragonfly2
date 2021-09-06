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

package hybrid

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver/local"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/gc"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

const StorageMode = storage.HybridStorageMode

const secureLevel = 500 * unit.MB

var _ storage.Manager = (*hybridStorageMgr)(nil)
var _ gc.Executor = (*hybridStorageMgr)(nil)

func init() {
	storage.Register(StorageMode, newStorageManager)
}

// NewStorageManager performs initialization for storage manager and return a storage Manager.
func newStorageManager(cfg *storage.Config) (storage.Manager, error) {
	if len(cfg.DriverConfigs) != 2 {
		return nil, fmt.Errorf("disk storage manager should have two driver, cfg's driver number is wrong : %v", cfg)
	}
	diskDriver, ok := storedriver.Get(local.DiskDriverName)
	if !ok {
		return nil, fmt.Errorf("can not find disk driver for hybrid storage manager, config is %v", cfg)
	}
	memoryDriver, ok := storedriver.Get(local.MemoryDriverName)
	if !ok {
		return nil, fmt.Errorf("can not find memory driver for hybrid storage manager, config %v", cfg)
	}
	storageMgr := &hybridStorageMgr{
		cfg:          cfg,
		memoryDriver: memoryDriver,
		diskDriver:   diskDriver,
		hasShm:       true,
		shmSwitch:    newShmSwitch(),
	}
	gc.Register("hybridStorage", cfg.GCInitialDelay, cfg.GCInterval, storageMgr)
	return storageMgr, nil
}

func (h *hybridStorageMgr) Initialize(taskMgr supervisor.SeedTaskMgr) {
	h.taskMgr = taskMgr
	diskGcConfig := h.cfg.DriverConfigs[local.DiskDriverName].GCConfig

	if diskGcConfig == nil {
		diskGcConfig = h.getDiskDefaultGcConfig()
		logger.GcLogger.With("type", "hybrid").Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	h.diskDriverCleaner, _ = storage.NewStorageCleaner(diskGcConfig, h.diskDriver, h, taskMgr)
	memoryGcConfig := h.cfg.DriverConfigs[local.MemoryDriverName].GCConfig
	if memoryGcConfig == nil {
		memoryGcConfig = h.getMemoryDefaultGcConfig()
		logger.GcLogger.With("type", "hybrid").Warnf("memory gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	h.memoryDriverCleaner, _ = storage.NewStorageCleaner(memoryGcConfig, h.memoryDriver, h, taskMgr)
	logger.GcLogger.With("type", "hybrid").Info("success initialize hybrid cleaners")
}

func (h *hybridStorageMgr) getDiskDefaultGcConfig() *storage.GCConfig {
	totalSpace, err := h.diskDriver.GetTotalSpace()
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of disk: %v", err)
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

func (h *hybridStorageMgr) getMemoryDefaultGcConfig() *storage.GCConfig {
	// determine whether the shared cache can be used
	diff := unit.Bytes(0)
	totalSpace, err := h.memoryDriver.GetTotalSpace()
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of memory: %v", err)
	}
	if totalSpace < 72*unit.GB {
		diff = 72*unit.GB - totalSpace
	}
	if diff >= totalSpace {
		h.hasShm = false
	}
	return &storage.GCConfig{
		YoungGCThreshold:  10*unit.GB + diff,
		FullGCThreshold:   2*unit.GB + diff,
		CleanRatio:        3,
		IntervalThreshold: 2 * time.Hour,
	}
}

type hybridStorageMgr struct {
	cfg                 *storage.Config
	memoryDriver        storedriver.Driver
	diskDriver          storedriver.Driver
	diskDriverCleaner   *storage.Cleaner
	memoryDriverCleaner *storage.Cleaner
	taskMgr             supervisor.SeedTaskMgr
	shmSwitch           *shmSwitch
	hasShm              bool
}

func (h *hybridStorageMgr) GC() error {
	logger.GcLogger.With("type", "hybrid").Info("start the hybrid storage gc job")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gcTaskIDs, err := h.diskDriverCleaner.GC("hybrid", false)
		if err != nil {
			logger.GcLogger.With("type", "hybrid").Error("gc disk: failed to get gcTaskIds")
		}
		realGCCount := h.gcTasks(gcTaskIDs, true)
		logger.GcLogger.With("type", "hybrid").Infof("at most %d tasks can be cleaned up from disk, actual gc %d tasks", len(gcTaskIDs), realGCCount)
	}()
	if h.hasShm {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gcTaskIDs, err := h.memoryDriverCleaner.GC("hybrid", false)
			logger.GcLogger.With("type", "hybrid").Infof("at most %d tasks can be cleaned up from memory", len(gcTaskIDs))
			if err != nil {
				logger.GcLogger.With("type", "hybrid").Error("gc memory: failed to get gcTaskIds")
			}
			h.gcTasks(gcTaskIDs, false)
		}()
	}
	wg.Wait()
	return nil
}

func (h *hybridStorageMgr) gcTasks(gcTaskIDs []string, isDisk bool) int {
	var realGCCount int
	for _, taskID := range gcTaskIDs {
		synclock.Lock(taskID, false)
		// try to ensure the taskID is not using again
		if _, exist := h.taskMgr.Exist(taskID); exist {
			synclock.UnLock(taskID, false)
			continue
		}
		realGCCount++
		if isDisk {
			if err := h.deleteDiskFiles(taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc disk: failed to delete disk files with taskID(%s): %v", taskID, err)
				synclock.UnLock(taskID, false)
				continue
			}
		} else {
			if err := h.deleteMemoryFiles(taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc memory: failed to delete memory files with taskID(%s): %v", taskID, err)
				synclock.UnLock(taskID, false)
				continue
			}
		}
		synclock.UnLock(taskID, false)
	}
	return realGCCount
}

func (h *hybridStorageMgr) WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error {
	raw := storage.GetDownloadRaw(taskID)
	raw.Offset = offset
	raw.Length = len
	return h.diskDriver.Put(raw, data)
}

func (h *hybridStorageMgr) DeleteTask(taskID string) error {
	return h.deleteTaskFiles(taskID, true, true)
}

func (h *hybridStorageMgr) ReadDownloadFile(taskID string) (io.ReadCloser, error) {
	return h.diskDriver.Get(storage.GetDownloadRaw(taskID))
}

func (h *hybridStorageMgr) ReadPieceMetaRecords(taskID string) ([]*storage.PieceMetaRecord, error) {
	readBytes, err := h.diskDriver.GetBytes(storage.GetPieceMetaDataRaw(taskID))
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

func (h *hybridStorageMgr) ReadFileMetaData(taskID string) (*storage.FileMetaData, error) {
	readBytes, err := h.diskDriver.GetBytes(storage.GetTaskMetaDataRaw(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "get metadata bytes")
	}

	metaData := &storage.FileMetaData{}
	if err := json.Unmarshal(readBytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "unmarshal metadata bytes")
	}
	return metaData, nil
}

func (h *hybridStorageMgr) AppendPieceMetaData(taskID string, record *storage.PieceMetaRecord) error {
	return h.diskDriver.PutBytes(storage.GetAppendPieceMetaDataRaw(taskID), []byte(record.String()+"\n"))
}

func (h *hybridStorageMgr) WriteFileMetaData(taskID string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "marshal metadata")
	}
	return h.diskDriver.PutBytes(storage.GetTaskMetaDataRaw(taskID), data)
}

func (h *hybridStorageMgr) WritePieceMetaRecords(taskID string, records []*storage.PieceMetaRecord) error {
	recordStrings := make([]string, 0, len(records))
	for i := range records {
		recordStrings = append(recordStrings, records[i].String())
	}
	return h.diskDriver.PutBytes(storage.GetPieceMetaDataRaw(taskID), []byte(strings.Join(recordStrings, "\n")))
}

func (h *hybridStorageMgr) CreateUploadLink(taskID string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(h.diskDriver.GetPath(storage.GetDownloadRaw(taskID)),
		h.diskDriver.GetPath(storage.GetUploadRaw(taskID))); err != nil {
		return err
	}
	return nil
}

func (h *hybridStorageMgr) ResetRepo(task *types.SeedTask) error {
	if err := h.deleteTaskFiles(task.TaskID, false, true); err != nil {
		logger.WithTaskID(task.TaskID).Errorf("reset repo: failed to delete task files: %v", err)
	}
	// 判断是否有足够空间存放
	shmPath, err := h.tryShmSpace(task.URL, task.TaskID, task.SourceFileLength)
	if err == nil {
		return fileutils.SymbolicLink(shmPath, h.diskDriver.GetPath(storage.GetDownloadRaw(task.TaskID)))
	}
	return nil
}

func (h *hybridStorageMgr) GetDownloadPath(rawFunc *storedriver.Raw) string {
	return h.diskDriver.GetPath(rawFunc)
}

func (h *hybridStorageMgr) StatDownloadFile(taskID string) (*storedriver.StorageInfo, error) {
	return h.diskDriver.Stat(storage.GetDownloadRaw(taskID))
}

func (h *hybridStorageMgr) TryFreeSpace(fileLength int64) (bool, error) {
	diskFreeSpace, err := h.diskDriver.GetFreeSpace()
	if err != nil {
		return false, err
	}
	if diskFreeSpace > 100*unit.GB && diskFreeSpace.ToNumber() > fileLength {
		return true, nil
	}

	remainder := atomic.NewInt64(0)
	r := &storedriver.Raw{
		WalkFn: func(filePath string, info os.FileInfo, err error) error {
			if fileutils.IsRegular(filePath) {
				taskID := strings.Split(path.Base(filePath), ".")[0]
				task, exist := h.taskMgr.Exist(taskID)
				if exist {
					var totalLen int64 = 0
					if task.CdnFileLength > 0 {
						totalLen = task.CdnFileLength
					} else {
						totalLen = task.SourceFileLength
					}
					if totalLen > 0 {
						remainder.Add(totalLen - info.Size())
					}
				}
			}
			return nil
		},
	}
	h.diskDriver.Walk(r)

	enoughSpace := diskFreeSpace.ToNumber()-remainder.Load() > fileLength
	if !enoughSpace {
		h.diskDriverCleaner.GC("hybrid", true)
		remainder.Store(0)
		h.diskDriver.Walk(r)
		diskFreeSpace, err = h.diskDriver.GetFreeSpace()
		if err != nil {
			return false, err
		}
		enoughSpace = diskFreeSpace.ToNumber()-remainder.Load() > fileLength
	}
	if !enoughSpace {
		return false, nil
	}

	return true, nil
}

func (h *hybridStorageMgr) deleteDiskFiles(taskID string) error {
	return h.deleteTaskFiles(taskID, true, true)
}

func (h *hybridStorageMgr) deleteMemoryFiles(taskID string) error {
	return h.deleteTaskFiles(taskID, true, false)
}

func (h *hybridStorageMgr) deleteTaskFiles(taskID string, deleteUploadPath bool, deleteHardLink bool) error {
	// delete task file data
	if err := h.diskDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	// delete memory file
	if err := h.memoryDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}

	if deleteUploadPath {
		if err := h.diskDriver.Remove(storage.GetUploadRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
	}
	exists := h.diskDriver.Exits(getHardLinkRaw(taskID))
	if !deleteHardLink && exists {
		if err := h.diskDriver.MoveFile(h.diskDriver.GetPath(getHardLinkRaw(taskID)), h.diskDriver.GetPath(storage.GetDownloadRaw(taskID))); err != nil {
			return err
		}
	} else {
		if err := h.diskDriver.Remove(getHardLinkRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
		// deleteTaskFiles delete files associated with taskID
		if err := h.diskDriver.Remove(storage.GetTaskMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
		// delete piece meta data
		if err := h.diskDriver.Remove(storage.GetPieceMetaDataRaw(taskID)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
	}
	// try to clean the parent bucket
	if err := h.diskDriver.Remove(storage.GetParentRaw(taskID)); err != nil &&
		!cdnerrors.IsFileNotExist(err) {
		logger.WithTaskID(taskID).Warnf("failed to remove parent bucket: %v", err)
	}
	return nil
}

func (h *hybridStorageMgr) tryShmSpace(url, taskID string, fileLength int64) (string, error) {
	if h.shmSwitch.check(url, fileLength) && h.hasShm {
		remainder := atomic.NewInt64(0)
		h.memoryDriver.Walk(&storedriver.Raw{
			WalkFn: func(filePath string, info os.FileInfo, err error) error {
				if fileutils.IsRegular(filePath) {
					taskID := strings.Split(path.Base(filePath), ".")[0]
					task, exist := h.taskMgr.Exist(taskID)
					if exist {
						var totalLen int64 = 0
						if task.CdnFileLength > 0 {
							totalLen = task.CdnFileLength
						} else {
							totalLen = task.SourceFileLength
						}
						if totalLen > 0 {
							remainder.Add(totalLen - info.Size())
						}
					}
				}
				return nil
			},
		})
		canUseShm := h.getMemoryUsableSpace()-unit.Bytes(remainder.Load())-secureLevel >= unit.Bytes(
			fileLength)
		if !canUseShm {
			// 如果剩余空间过小，则强制执行一次fullgc后在检查是否满足
			h.memoryDriverCleaner.GC("hybrid", true)
			canUseShm = h.getMemoryUsableSpace()-unit.Bytes(remainder.Load())-secureLevel >= unit.Bytes(
				fileLength)
		}
		if canUseShm { // 创建shm
			raw := &storedriver.Raw{
				Key: taskID,
			}
			return h.memoryDriver.GetPath(raw), nil
		}
		return "", fmt.Errorf("not enough free space left")
	}
	return "", fmt.Errorf("shared memory is not allowed")
}

func (h *hybridStorageMgr) getMemoryUsableSpace() unit.Bytes {
	totalSize, freeSize, err := h.memoryDriver.GetTotalAndFreeSpace()
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total and free space of memory: %v", err)
		return 0
	}
	// 如果内存总容量大于等于 72G，则返回内存的剩余可用空间
	threshold := 72 * unit.GB
	if totalSize >= threshold {
		return freeSize
	}
	// 如果总容量小于72G， 如40G容量，则可用空间为 当前可用空间 - 32G： 最大可用空间为8G，50G容量，则可用空间为 当前可用空间 - 22G：最大可用空间为28G

	usableSpace := freeSize - (72*unit.GB - totalSize)
	if usableSpace > 0 {
		return usableSpace
	}
	return 0
}

func getHardLinkRaw(taskID string) *storedriver.Raw {
	raw := storage.GetDownloadRaw(taskID)
	raw.Key = raw.Key + ".hard"
	return raw
}
