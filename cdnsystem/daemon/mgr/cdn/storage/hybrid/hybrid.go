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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
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
	"go.uber.org/atomic"
)

const name = "hybrid"

const secureLevel = 500 * unit.MB

func init() {
	var builder *hybridBuilder = nil
	var _ storage.Builder = builder

	var hybrid *hybridStorageMgr = nil
	var _ storage.Manager = hybrid
	var _ gc.Executor = hybrid
}

type hybridBuilder struct {
}

func (*hybridBuilder) Build(cfg *config.Config) (storage.Manager, error) {
	diskStore, err := storedriver.Get(local.StorageDriver)
	if err != nil {
		return nil, err
	}
	memoryStore, err := storedriver.Get(local.MemoryStorageDriver)
	if err != nil {
		return nil, err
	}
	storageMgr := &hybridStorageMgr{
		memoryStore: memoryStore,
		diskStore:   diskStore,
		hasShm:      true,
		shmSwitch:   newShmSwitch(),
	}
	gc.Register("hybridStorage", cfg.GCInitialDelay, cfg.GCStorageInterval, storageMgr)
	return storageMgr, nil
}

func (*hybridBuilder) Name() string {
	return name
}

type hybridStorageMgr struct {
	cfg                config.Config
	memoryStore        storedriver.Driver
	diskStore          storedriver.Driver
	taskMgr            mgr.SeedTaskMgr
	diskStoreCleaner   *storage.Cleaner
	memoryStoreCleaner *storage.Cleaner
	shmSwitch          *shmSwitch
	hasShm             bool
}

func (h *hybridStorageMgr) GC(ctx context.Context) error {
	logger.GcLogger.With("type", "hybrid").Info("start the hybrid storage gc job")
	go func() {
		gcTaskIDs, err := h.diskStoreCleaner.Gc(ctx, "hybrid", false)
		if err != nil {
			logger.GcLogger.With("type", "hybrid").Error("gc disk: failed to get gcTaskIds")
		}
		logger.GcLogger.With("type", "hybrid").Infof("at most %d tasks can be cleaned up from disk", len(gcTaskIDs))
		h.gcTasks(ctx, gcTaskIDs, true)
	}()
	if h.hasShm {
		go func() {
			gcTaskIDs, err := h.memoryStoreCleaner.Gc(ctx, "hybrid", false)
			logger.GcLogger.With("type", "hybrid").Infof("at most %d tasks can be cleaned up from memory", len(gcTaskIDs))
			if err != nil {
				logger.GcLogger.With("type", "hybrid").Error("gc memory: failed to get gcTaskIds")
			}
			h.gcTasks(ctx, gcTaskIDs, false)
		}()
	}
	return nil
}

func (h *hybridStorageMgr) gcTasks(ctx context.Context, gcTaskIDs []string, isDisk bool) {
	for _, taskID := range gcTaskIDs {
		synclock.Lock(taskID, false)
		// try to ensure the taskID is not using again
		if _, err := h.taskMgr.Get(ctx, taskID); err == nil || !cdnerrors.IsDataNotFound(err) {
			if err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc disk: failed to get taskID(%s): %v", taskID, err)
			}
			synclock.UnLock(taskID, false)
			continue
		}
		if isDisk {
			if err := h.deleteDiskFiles(ctx, taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc disk: failed to delete disk files with taskID(%s): %v", taskID, err)
				synclock.UnLock(taskID, false)
				continue
			}
		} else {
			if err := h.deleteMemoryFiles(ctx, taskID); err != nil {
				logger.GcLogger.With("type", "hybrid").Errorf("gc memory: failed to delete memory files with taskID(%s): %v", taskID, err)
				synclock.UnLock(taskID, false)
				continue
			}
		}
		synclock.UnLock(taskID, false)
	}
}

func (h *hybridStorageMgr) SetTaskMgr(taskMgr mgr.SeedTaskMgr) {
	h.taskMgr = taskMgr
}

func (h *hybridStorageMgr) InitializeCleaners() {
	diskGcConfig := h.diskStore.GetGcConfig(context.TODO())
	if diskGcConfig == nil {
		diskGcConfig = h.getDiskDefaultGcConfig()
		logger.GcLogger.With("type", "hybrid").Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}

	h.diskStoreCleaner = storage.NewStorageCleaner(diskGcConfig, h.diskStore, h, h.taskMgr)
	memoryGcConfig := h.memoryStore.GetGcConfig(context.TODO())
	if memoryGcConfig == nil {
		memoryGcConfig = h.getMemoryDefaultGcConfig()
		logger.GcLogger.With("type", "hybrid").Warnf("memory gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	h.memoryStoreCleaner = storage.NewStorageCleaner(memoryGcConfig, h.memoryStore, h, h.taskMgr)
	logger.GcLogger.With("type", "hybrid").Info("success initialize hybrid cleaners")
}

func (h *hybridStorageMgr) WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64,
	buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskId)
	raw.Offset = offset
	raw.Length = len
	return h.diskStore.Put(ctx, raw, buf)
}

func (h *hybridStorageMgr) DeleteTask(ctx context.Context, taskId string) error {
	return h.deleteTaskFiles(ctx, taskId, true, true)
}

func (h *hybridStorageMgr) ReadDownloadFile(ctx context.Context, taskId string) (io.ReadCloser, error) {
	return h.diskStore.Get(ctx, storage.GetDownloadRaw(taskId))
}

func (h *hybridStorageMgr) ReadPieceMetaRecords(ctx context.Context, taskId string) ([]*storage.PieceMetaRecord, error) {
	bytes, err := h.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	var result = make([]*storage.PieceMetaRecord, 0)
	for _, pieceStr := range pieceMetaRecords {
		record, err := storage.ParsePieceMetaRecord(pieceStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get piece meta record:%v", pieceStr)
		}
		result = append(result, record)
	}
	return result, nil
}

func (h *hybridStorageMgr) ReadFileMetaData(ctx context.Context, taskId string) (*storage.FileMetaData, error) {
	bytes, err := h.diskStore.GetBytes(ctx, storage.GetTaskMetaDataRaw(taskId))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &storage.FileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	return metaData, nil
}

func (h *hybridStorageMgr) AppendPieceMetaData(ctx context.Context, taskId string, record *storage.PieceMetaRecord) error {
	return h.diskStore.PutBytes(ctx, storage.GetAppendPieceMetaDataRaw(taskId), []byte(record.String()+"\n"))
}

func (h *hybridStorageMgr) WriteFileMetaData(ctx context.Context, taskId string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return h.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskId), data)
}

func (h *hybridStorageMgr) WritePieceMetaRecords(ctx context.Context, taskId string, records []*storage.PieceMetaRecord) error {
	recordStrs := make([]string, 0, len(records))
	for i := range records {
		recordStrs = append(recordStrs, records[i].String())
	}
	return h.diskStore.PutBytes(ctx, storage.GetPieceMetaDataRaw(taskId), []byte(strings.Join(recordStrs, "\n")))
}

func (h *hybridStorageMgr) CreateUploadLink(ctx context.Context, taskId string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(h.diskStore.GetPath(storage.GetDownloadRaw(taskId)),
		h.diskStore.GetPath(storage.GetUploadRaw(taskId))); err != nil {
		return err
	}
	return nil
}

func (h *hybridStorageMgr) ResetRepo(ctx context.Context, task *types.SeedTask) error {
	if err := h.deleteTaskFiles(ctx, task.TaskId, false, true); err != nil {
		logger.WithTaskID(task.TaskId).Errorf("reset repo: failed to delete task files: %v", err)
	}
	// 判断是否有足够空间存放
	shmPath, err := h.tryShmSpace(ctx, task.Url, task.TaskId, task.SourceFileLength)
	if err == nil {
		return fileutils.SymbolicLink(shmPath, h.diskStore.GetPath(storage.GetDownloadRaw(task.TaskId)))
	}
	return nil
}

func (h *hybridStorageMgr) GetDownloadPath(rawFunc *storedriver.Raw) string {
	return h.diskStore.GetPath(rawFunc)
}

func (h *hybridStorageMgr) StatDownloadFile(ctx context.Context, taskId string) (*storedriver.StorageInfo, error) {
	return h.diskStore.Stat(ctx, storage.GetDownloadRaw(taskId))
}

func (h *hybridStorageMgr) deleteDiskFiles(ctx context.Context, taskId string) error {
	return h.deleteTaskFiles(ctx, taskId, true, true)
}

func (h *hybridStorageMgr) deleteMemoryFiles(ctx context.Context, taskId string) error {
	return h.deleteTaskFiles(ctx, taskId, true, false)
}

func (h *hybridStorageMgr) deleteTaskFiles(ctx context.Context, taskId string, deleteUploadPath bool, deleteHardLink bool) error {
	// delete task file data
	if err := h.diskStore.Remove(ctx, storage.GetDownloadRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}
	// delete memory file
	if err := h.memoryStore.Remove(ctx, storage.GetDownloadRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
		return err
	}

	if deleteUploadPath {
		if err := h.diskStore.Remove(ctx, storage.GetUploadRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
	}
	exists := h.diskStore.Exits(ctx, getHardLinkRaw(taskId))
	if !deleteHardLink && exists {
		h.diskStore.MoveFile(h.diskStore.GetPath(getHardLinkRaw(taskId)), h.diskStore.GetPath(storage.GetDownloadRaw(
			taskId)))
	} else {
		if err := h.diskStore.Remove(ctx, getHardLinkRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
		// deleteTaskFiles delete files associated with taskId
		if err := h.diskStore.Remove(ctx, storage.GetTaskMetaDataRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
		// delete piece meta data
		if err := h.diskStore.Remove(ctx, storage.GetPieceMetaDataRaw(taskId)); err != nil && !cdnerrors.IsFileNotExist(err) {
			return err
		}
	}
	// try to clean the parent bucket
	if err := h.diskStore.Remove(ctx, storage.GetParentRaw(taskId)); err != nil &&
		!cdnerrors.IsFileNotExist(err) {
		logger.WithTaskID(taskId).Warnf("failed to remove parent bucket:%v", err)
	}
	return nil
}

func (h *hybridStorageMgr) tryShmSpace(ctx context.Context, url, taskId string, fileLength int64) (string, error) {
	if h.shmSwitch.check(url, fileLength) && h.hasShm {
		remainder := atomic.NewInt64(0)
		h.memoryStore.Walk(ctx, &storedriver.Raw{
			WalkFn: func(filePath string, info os.FileInfo, err error) error {
				if fileutils.IsRegular(filePath) {
					taskId := path.Base(filePath)
					task, err := h.taskMgr.Get(ctx, taskId)
					if err == nil {
						var totalLen int64 = 0
						if task.CdnFileLength > 0 {
							totalLen = task.CdnFileLength
						} else {
							totalLen = task.SourceFileLength
						}
						if totalLen > 0 {
							remainder.Add(totalLen - info.Size())
						}
					} else {
						logger.Warnf("failed to get task:%s: %v", taskId, err)
					}
				}
				return nil
			},
		})
		canUseShm := h.getMemoryUsableSpace(ctx)-unit.Bytes(remainder.Load())-secureLevel >= unit.Bytes(
			fileLength)
		if !canUseShm {
			// 如果剩余空间过小，则强制执行一次fullgc后在检查是否满足
			h.memoryStoreCleaner.Gc(ctx, "hybrid", true)
			canUseShm = h.getMemoryUsableSpace(ctx)-unit.Bytes(remainder.Load())-secureLevel >= unit.Bytes(
				fileLength)
		}
		if canUseShm { // 创建shm
			raw := &storedriver.Raw{
				Key: taskId,
			}
			return h.memoryStore.GetPath(raw), nil
		}
		return "", fmt.Errorf("not enough free space left")
	}
	return "", fmt.Errorf("shared memory is not allowed")
}

func (h *hybridStorageMgr) getDiskDefaultGcConfig() *storedriver.GcConfig {
	totalSpace, err := h.diskStore.GetTotalSpace(context.TODO())
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of disk: %v", err)
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

func (h *hybridStorageMgr) getMemoryDefaultGcConfig() *storedriver.GcConfig {
	// determine whether the shared cache can be used
	diff := unit.Bytes(0)
	totalSpace, err := h.memoryStore.GetTotalSpace(context.TODO())
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of memory: %v", err)
	}
	if totalSpace < 72*unit.GB {
		diff = 72*unit.GB - totalSpace
	}
	if diff >= totalSpace {
		h.hasShm = false
	}
	return &storedriver.GcConfig{
		YoungGCThreshold:  10*unit.GB + diff,
		FullGCThreshold:   2*unit.GB + diff,
		CleanRatio:        3,
		IntervalThreshold: 2 * time.Hour,
	}
}

func (h *hybridStorageMgr) getMemoryUsableSpace(ctx context.Context) unit.Bytes {
	totalSize, freeSize, err := h.memoryStore.GetTotalAndFreeSpace(ctx)
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

func getHardLinkRaw(taskId string) *storedriver.Raw {
	raw := storage.GetDownloadRaw(taskId)
	raw.Key = raw.Key + ".hard"
	return raw
}

func init() {
	storage.Register(&hybridBuilder{})
}
