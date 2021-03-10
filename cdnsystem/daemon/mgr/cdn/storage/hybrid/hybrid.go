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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/store/disk"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"io"
	"os"
	"path"
	"strings"
)

const name = "hybrid"

const SecureLevel = 500 * fileutils.MB

type hybridBuilder struct {
}

func (*hybridBuilder) Build() (storage.StorageMgr, error) {
	diskStore, err := store.Get(disk.StorageDriver)
	if err != nil {
		return nil, err
	}
	memoryStore, err := store.Get(disk.MemoryStorageDriver)
	if err != nil {
		return nil, err
	}
	return &hybridStorage{
		memoryStore: memoryStore,
		diskStore:   diskStore,
		shmSwitch:   newShmSwitch(),
	}, nil
}

func (*hybridBuilder) Name() string {
	return name
}

type hybridStorage struct {
	cfg                config.Config
	memoryStore        store.StorageDriver
	diskStore          store.StorageDriver
	taskMgr            mgr.SeedTaskMgr
	diskStoreCleaner   *storage.Cleaner
	memoryStoreCleaner *storage.Cleaner
	shmSwitch          *shmSwitch
	hasShm             bool
}

func (h *hybridStorage) Gc(ctx context.Context) {
	go h.diskStoreCleaner.Gc(ctx, false)
	if h.hasShm {
		go h.memoryStoreCleaner.Gc(ctx, false)
	}
}

func (h *hybridStorage) SetTaskMgr(taskMgr mgr.SeedTaskMgr) {
	h.taskMgr = taskMgr
}

func (h *hybridStorage) InitializeCleaners() {
	diskGcConfig := h.diskStore.GetGcConfig(context.TODO())
	if diskGcConfig == nil {
		diskGcConfig = h.getDiskDefaultGcConfig()
		logger.GcLogger.Warnf("disk gc config is nil, use default gcConfig: %v", diskGcConfig)
	}

	h.diskStoreCleaner = &storage.Cleaner{
		Cfg:        diskGcConfig,
		Store:      h.diskStore,
		StorageMgr: h,
		TaskMgr:    h.taskMgr,
	}
	memoryGcConfig := h.memoryStore.GetGcConfig(context.TODO())
	if memoryGcConfig == nil {
		memoryGcConfig = h.getMemoryDefaultGcConfig()
		logger.GcLogger.Warnf("memory gc config is nil, use default gcConfig: %v", diskGcConfig)
	}
	h.memoryStoreCleaner = &storage.Cleaner{
		Cfg:        memoryGcConfig,
		Store:      h.memoryStore,
		StorageMgr: h,
		TaskMgr:    h.taskMgr,
	}
	logger.GcLogger.Info("success initialize hybrid cleaners")
}

func (h *hybridStorage) WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64,
	buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskId)
	raw.Offset = offset
	raw.Length = len
	return h.diskStore.Put(ctx, raw, buf)
}

func (h *hybridStorage) DeleteTask(ctx context.Context, taskId string) error {
	return h.deleteTaskFiles(ctx, taskId, true, true)
}

func (h *hybridStorage) ReadDownloadFile(ctx context.Context, taskId string) (io.Reader, error) {
	return h.diskStore.Get(ctx, storage.GetDownloadRaw(taskId))
}

func (h *hybridStorage) ReadPieceMetaRecords(ctx context.Context, taskId string) ([]*storage.PieceMetaRecord, error) {
	bytes, err := h.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
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

func (h *hybridStorage) ReadFileMetaData(ctx context.Context, taskId string) (*storage.FileMetaData, error) {
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

func (h *hybridStorage) AppendPieceMetaData(ctx context.Context, taskId string, record *storage.PieceMetaRecord) error {
	data := getPieceMetaValue(record)
	return h.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), []byte(data+"\n"))
}

func (h *hybridStorage) WriteFileMetaData(ctx context.Context, taskId string, metaData *storage.FileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return h.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskId), data)
}

func (h *hybridStorage) CreateUploadLink(ctx context.Context, taskId string) error {
	// create a soft link from the upload file to the download file
	if err := fileutils.SymbolicLink(h.diskStore.GetPath(storage.GetDownloadRaw(taskId)),
		h.diskStore.GetPath(storage.GetUploadRaw(taskId))); err != nil {
		return err
	}
	return nil
}

func (h *hybridStorage) ResetRepo(ctx context.Context, task *types.SeedTask) error {
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

func (h *hybridStorage) GetDownloadPath(rawFunc *store.Raw) string {
	return h.diskStore.GetPath(rawFunc)
}

func (h *hybridStorage) StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error) {
	return h.diskStore.Stat(ctx, storage.GetDownloadRaw(taskId))
}

func (h *hybridStorage) deleteTaskFiles(ctx context.Context, taskId string, deleteUploadPath bool, deleteHardLink bool) error {
	// delete task file data
	if err := h.diskStore.Remove(ctx, storage.GetDownloadRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		return err
	}
	// delete memory file
	if err := h.memoryStore.Remove(ctx, storage.GetDownloadRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
		return err
	}

	if deleteUploadPath {
		if err := h.diskStore.Remove(ctx, storage.GetUploadRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
			return err
		}
	}
	exists := h.diskStore.Exits(ctx, getHardLinkRaw(taskId))
	if !deleteHardLink && exists {
		h.diskStore.MoveFile(h.diskStore.GetPath(getHardLinkRaw(taskId)), h.diskStore.GetPath(storage.GetDownloadRaw(
			taskId)))
	} else {
		if err := h.diskStore.Remove(ctx, getHardLinkRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
			return err
		}
		// deleteTaskFiles delete files associated with taskId
		if err := h.diskStore.Remove(ctx, storage.GetTaskMetaDataRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
			return err
		}
		// delete piece meta data
		if err := h.diskStore.Remove(ctx, storage.GetPieceMetaDataRaw(taskId)); err != nil && !cdnerrors.IsKeyNotFound(err) {
			return err
		}
	}
	// try to clean the parent bucket
	if err := h.diskStore.Remove(ctx, storage.GetParentRaw(taskId)); err != nil &&
		!cdnerrors.IsKeyNotFound(err) {
		logger.WithTaskID(taskId).Warnf("failed to remove parent bucket:%v", err)
	}
	return nil
}

func (h *hybridStorage) tryShmSpace(ctx context.Context, url, taskId string, fileLength int64) (string, error) {
	if h.shmSwitch.check(url, fileLength) && h.hasShm {
		remainder := atomic.NewInt64(0)
		h.memoryStore.Walk(ctx, &store.Raw{
			WalkFn: func(filePath string, info os.FileInfo, err error) error {
				if fileutils.IsRegularFile(filePath) {
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
		canUseShm := h.getMemoryUsableSpace(ctx)-fileutils.Fsize(remainder.Load())-SecureLevel >= fileutils.Fsize(
			fileLength)
		if !canUseShm {
			// 如果剩余空间过小，则强制执行一次fullgc后在检查是否满足
			h.memoryStoreCleaner.Gc(ctx, true)
			canUseShm = h.getMemoryUsableSpace(ctx)-fileutils.Fsize(remainder.Load())-SecureLevel >= fileutils.Fsize(fileLength)
		}
		if canUseShm { // 创建shm
			raw := &store.Raw{
				Key: taskId,
			}
			return h.memoryStore.GetPath(raw), nil
		}
		return "", fmt.Errorf("not enough free space left")
	}
	return "", fmt.Errorf("shared memory is not allowed")
}

func (h *hybridStorage) getMemoryUsableSpace(ctx context.Context) fileutils.Fsize {
	totalSize, freeSize, err := h.memoryStore.GetTotalAndFreeSpace(ctx)
	if err != nil {
		logger.GcLogger.Errorf("failed to get total and free space of memory: %v", err)
		return 0
	}
	// 如果内存总容量大于等于 72G，则返回内存的剩余可用空间
	threshold := 72 * fileutils.GB
	if totalSize >= threshold {
		return freeSize
	}
	// 如果总容量小于72G， 如40G容量，则可用空间为 当前可用空间 - 32G： 最大可用空间为8G，50G容量，则可用空间为 当前可用空间 - 22G：最大可用空间为28G

	usableSpace := freeSize - (72*fileutils.GB - totalSize)
	if usableSpace > 0 {
		return usableSpace
	}
	return 0
}

func getHardLinkRaw(taskId string) *store.Raw {
	raw := storage.GetDownloadRaw(taskId)
	raw.Key = raw.Key + ".hard"
	return raw
}

func init() {
	storage.Register(&hybridBuilder{})
}
