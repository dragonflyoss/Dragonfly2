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

	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/cdn/gc"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgsync "d7y.io/dragonfly/v2/pkg/sync"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
)

const _hybrid = "hybrid"

const secureLevel = 500 * unit.MB

var (
	_ storage.Manager = (*hybridStorageManager)(nil)
	_ gc.Executor     = (*hybridStorageManager)(nil)
	_ storage.Builder = (*hybridStorageBuilder)(nil)
)

type hybridStorageBuilder struct {
	defaultDriverConfigsFunc func() map[string]*storage.DriverConfig
	validateFunc             func(map[string]*storage.DriverConfig) []error
}

func (*hybridStorageBuilder) Build(config storage.Config, taskManager task.Manager) (storage.Manager, error) {
	diskDriverBuilder := storedriver.Get(local.DiskDriverName)
	if diskDriverBuilder == nil {
		return nil, fmt.Errorf("can not find disk driver for hybrid storage manager")
	}
	diskDriver, err := diskDriverBuilder(&storedriver.Config{
		BaseDir: config.DriverConfigs[local.DiskDriverName].BaseDir,
	})
	if err != nil {
		return nil, err
	}
	memoryDriverBuilder := storedriver.Get(local.MemoryDriverName)
	if memoryDriverBuilder == nil {
		return nil, fmt.Errorf("can not find memory driver for hybrid storage manager")
	}
	memoryDriver, err := memoryDriverBuilder(&storedriver.Config{
		BaseDir: config.DriverConfigs[local.MemoryDriverName].BaseDir,
	})
	if err != nil {
		return nil, err
	}
	storageManager := &hybridStorageManager{
		config:       config,
		memoryDriver: memoryDriver,
		diskDriver:   diskDriver,
		taskManager:  taskManager,
		shmSwitch:    newShmSwitch(),
		hasShm:       true,
		kmu:          pkgsync.NewKrwmutex(),
	}

	diskDriverCleaner, err := storage.NewStorageCleaner(config.DriverConfigs[local.DiskDriverName].DriverGCConfig, diskDriver, storageManager, taskManager)
	if err != nil {
		return nil, err
	}
	memoryDriverCleaner, err := storage.NewStorageCleaner(config.DriverConfigs[local.MemoryDriverName].DriverGCConfig, memoryDriver, storageManager, taskManager)
	if err != nil {
		return nil, err
	}
	storageManager.diskDriverCleaner = diskDriverCleaner
	storageManager.memoryDriverCleaner = memoryDriverCleaner

	gc.Register("hybridStorage", config.GCInitialDelay, config.GCInterval, storageManager)
	return storageManager, nil
}

func (*hybridStorageBuilder) Name() string {
	return _hybrid
}

func (b *hybridStorageBuilder) Validate(driverConfigs map[string]*storage.DriverConfig) []error {
	return b.validateFunc(driverConfigs)
}

func (b *hybridStorageBuilder) DefaultDriverConfigs() map[string]*storage.DriverConfig {
	return b.defaultDriverConfigsFunc()
}

func init() {
	storage.Register(&hybridStorageBuilder{
		defaultDriverConfigsFunc: getDefaultDriverGCConfigs,
		validateFunc:             validateConfig,
	})
}

type hybridStorageManager struct {
	config              storage.Config
	memoryDriver        storedriver.Driver
	diskDriver          storedriver.Driver
	diskDriverCleaner   storage.Cleaner
	memoryDriverCleaner storage.Cleaner
	taskManager         task.Manager
	shmSwitch           *shmSwitch
	// whether enable shm
	hasShm bool
	kmu    *pkgsync.Krwmutex
}

func (h *hybridStorageManager) WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error {
	raw := storage.GetDownloadRaw(taskID)
	raw.Offset = offset
	raw.Length = len
	return h.diskDriver.Put(raw, data)
}

func (h *hybridStorageManager) DeleteTask(taskID string) error {
	return h.deleteTaskFiles(taskID, true)
}

func (h *hybridStorageManager) ReadDownloadFile(taskID string) (io.ReadCloser, error) {
	return h.diskDriver.Get(storage.GetDownloadRaw(taskID))
}

func (h *hybridStorageManager) ReadPieceMetaRecords(taskID string) ([]*storage.PieceMetaRecord, error) {
	readBytes, err := h.diskDriver.GetBytes(storage.GetPieceMetadataRaw(taskID))
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

func (h *hybridStorageManager) ReadFileMetadata(taskID string) (*storage.FileMetadata, error) {
	readBytes, err := h.diskDriver.GetBytes(storage.GetTaskMetadataRaw(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "get metadata bytes")
	}

	metadata := &storage.FileMetadata{}
	if err := json.Unmarshal(readBytes, metadata); err != nil {
		return nil, errors.Wrapf(err, "unmarshal metadata bytes")
	}
	return metadata, nil
}

func (h *hybridStorageManager) AppendPieceMetadata(taskID string, record *storage.PieceMetaRecord) error {
	return h.diskDriver.PutBytes(storage.GetAppendPieceMetadataRaw(taskID), []byte(record.String()+"\n"))
}

func (h *hybridStorageManager) WriteFileMetadata(taskID string, metadata *storage.FileMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "marshal metadata")
	}
	return h.diskDriver.PutBytes(storage.GetTaskMetadataRaw(taskID), data)
}

func (h *hybridStorageManager) WritePieceMetaRecords(taskID string, records []*storage.PieceMetaRecord) error {
	recordStrings := make([]string, 0, len(records))
	for i := range records {
		recordStrings = append(recordStrings, records[i].String())
	}
	return h.diskDriver.PutBytes(storage.GetPieceMetadataRaw(taskID), []byte(strings.Join(recordStrings, "\n")))
}

func (h *hybridStorageManager) ResetRepo(seedTask *task.SeedTask) error {
	if err := h.deleteTaskFiles(seedTask.ID, true); err != nil {
		return errors.Errorf("delete task %s files: %v", seedTask.ID, err)
	}
	// 判断是否有足够空间存放
	if shmPath, err := h.tryShmSpace(seedTask.RawURL, seedTask.ID, seedTask.SourceFileLength); err != nil {
		if _, err := os.Create(h.diskDriver.GetPath(storage.GetDownloadRaw(seedTask.ID))); err != nil {
			return err
		}
	} else {
		if err := fileutils.SymbolicLink(shmPath, h.diskDriver.GetPath(storage.GetDownloadRaw(seedTask.ID))); err != nil {
			return err
		}
	}
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(h.diskDriver.GetPath(storage.GetDownloadRaw(seedTask.ID)),
		h.diskDriver.GetPath(storage.GetUploadRaw(seedTask.ID))); err != nil {
		return err
	}
	return nil
}

func (h *hybridStorageManager) GetDownloadPath(rawFunc *storedriver.Raw) string {
	return h.diskDriver.GetPath(rawFunc)
}

func (h *hybridStorageManager) StatDownloadFile(taskID string) (*storedriver.StorageInfo, error) {
	return h.diskDriver.Stat(storage.GetDownloadRaw(taskID))
}

func (h *hybridStorageManager) TryFreeSpace(fileLength int64) (bool, error) {
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
				seedTask, exist := h.taskManager.Exist(taskID)
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
	if err := h.diskDriver.Walk(r); err != nil {
		return false, err
	}

	enoughSpace := diskFreeSpace.ToNumber()-remainder.Load() > (fileLength + int64(5*unit.GB))
	if !enoughSpace {
		if _, err := h.diskDriverCleaner.GC("hybrid", true); err != nil {
			return false, err
		}

		remainder.Store(0)
		if err := h.diskDriver.Walk(r); err != nil {
			return false, err
		}
		diskFreeSpace, err = h.diskDriver.GetFreeSpace()
		if err != nil {
			return false, err
		}
		enoughSpace = diskFreeSpace.ToNumber()-remainder.Load() > (fileLength + int64(5*unit.GB))
	}
	if !enoughSpace {
		return false, nil
	}

	return true, nil
}

func (h *hybridStorageManager) deleteDiskFiles(taskID string) error {
	return h.deleteTaskFiles(taskID, true)
}

func (h *hybridStorageManager) deleteMemoryFiles(taskID string) error {
	return h.deleteTaskFiles(taskID, false)
}

func (h *hybridStorageManager) deleteTaskFiles(taskID string, deleteHardLink bool) error {
	// delete task file data
	if err := h.diskDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// delete memory file
	if err := h.memoryDriver.Remove(storage.GetDownloadRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// delete upload file
	if err := h.diskDriver.Remove(storage.GetUploadRaw(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	exists := h.diskDriver.Exits(getHardLinkRaw(taskID))
	if !deleteHardLink && exists {
		if err := h.diskDriver.MoveFile(h.diskDriver.GetPath(getHardLinkRaw(taskID)), h.diskDriver.GetPath(storage.GetDownloadRaw(taskID))); err != nil {
			return err
		}
	} else {
		if err := h.diskDriver.Remove(getHardLinkRaw(taskID)); err != nil && !os.IsNotExist(err) {
			return err
		}
		// deleteTaskFiles delete files associated with taskID
		if err := h.diskDriver.Remove(storage.GetTaskMetadataRaw(taskID)); err != nil && !os.IsNotExist(err) {
			return err
		}
		// delete piece meta data
		if err := h.diskDriver.Remove(storage.GetPieceMetadataRaw(taskID)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// try to clean the parent bucket
	if err := h.diskDriver.Remove(storage.GetParentRaw(taskID)); err != nil &&
		!os.IsNotExist(err) {
		logger.WithTaskID(taskID).Warnf("failed to remove parent bucket: %v", err)
	}
	return nil
}

func (h *hybridStorageManager) tryShmSpace(url, taskID string, fileLength int64) (string, error) {
	if h.shmSwitch.check(url, fileLength) && h.hasShm {
		remainder := atomic.NewInt64(0)
		if err := h.memoryDriver.Walk(&storedriver.Raw{
			WalkFn: func(filePath string, info os.FileInfo, err error) error {
				if fileutils.IsRegular(filePath) {
					taskID := strings.Split(path.Base(filePath), ".")[0]
					seedTask, exist := h.taskManager.Exist(taskID)
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
		}); err != nil {
			return "", err
		}

		canUseShm := h.getMemoryUsableSpace()-unit.Bytes(remainder.Load())-secureLevel >= unit.Bytes(
			fileLength)
		if !canUseShm {
			// 如果剩余空间过小，则强制执行一次fullgc后在检查是否满足
			if _, err := h.memoryDriverCleaner.GC("hybrid", true); err != nil {
				return "", err
			}

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

func (h *hybridStorageManager) GC() error {
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

func (h *hybridStorageManager) gcTasks(gcTaskIDs []string, isDisk bool) int {
	var realGCCount int
	for _, taskID := range gcTaskIDs {
		// try to ensure the taskID is not using again
		if _, exist := h.taskManager.Exist(taskID); exist {
			continue
		}
		realGCCount++
		h.kmu.Lock(taskID)
		if isDisk {
			if err := h.deleteDiskFiles(taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc disk: failed to delete disk files with taskID(%s): %v", taskID, err)
				h.kmu.Unlock(taskID)
				continue
			}
		} else {
			if err := h.deleteMemoryFiles(taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc memory: failed to delete memory files with taskID(%s): %v", taskID, err)
				h.kmu.Unlock(taskID)
				continue
			}
		}
		h.kmu.Unlock(taskID)
	}
	return realGCCount
}

func (h *hybridStorageManager) getMemoryUsableSpace() unit.Bytes {
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
