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
	"github.com/pkg/errors"
	"io"
)

const name = "hybrid"

type hybridBuilder struct {
}

func (*hybridBuilder) Build(buildOpts storage.BuildOptions) (storage.Storage,
	error) {
	diskStore, err := store.Get(disk.StorageDriver)
	if err != nil {
		return nil, err
	}
	memoryStore, err := store.Get(disk.MemoryStorageDriver)
	if err != nil {
		return nil, err
	}
	storage := &hybridStorage{
		memoryStore: memoryStore,
		diskStore:   diskStore,
	}
	return storage, nil
}

func (*hybridBuilder) Name() string {
	return name
}

type hybridStorage struct {
	memoryStore *store.Store
	diskStore   *store.Store
	shmMgr      *ShareMemManager
	cfg         config.Config
}

func (h *hybridStorage) Gc(ctx context.Context) {
	panic("implement me")
}

func (h *hybridStorage) SetTaskMgr(taskMgr mgr.SeedTaskMgr) {
	h.shmMgr = newShareMemManager(taskMgr, h.memoryStore)
}

func (h *hybridStorage) Walk(ctx context.Context, raw *store.Raw) error {
	return nil
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

func (h *hybridStorage) ReadPieceMetaBytes(ctx context.Context, taskId string) ([]byte, error) {
	return h.diskStore.GetBytes(ctx, storage.GetPieceMetaDataRaw(taskId))
}

func (h *hybridStorage) ReadFileMetaDataBytes(ctx context.Context, taskId string) ([]byte, error) {
	return h.diskStore.GetBytes(ctx, storage.GetTaskMetaDataRaw(taskId))
}

func (h *hybridStorage) AppendPieceMetaDataBytes(ctx context.Context, taskId string, bytes []byte) error {
	return h.diskStore.AppendBytes(ctx, storage.GetPieceMetaDataRaw(taskId), bytes)
}

func (h *hybridStorage) WriteFileMetaDataBytes(ctx context.Context, taskId string, data []byte) error {
	return h.diskStore.PutBytes(ctx, storage.GetTaskMetaDataRaw(taskId), data)
}

func (h *hybridStorage) CreateUploadLink(taskId string) error {
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
	shmPath, err := h.shmMgr.tryShmSpace(ctx, task.Url, task.TaskId, task.SourceFileLength)
	if err == nil {
		fileutils.SymbolicLink(shmPath, h.diskStore.GetPath(storage.GetDownloadRaw(task.TaskId)))
	} else {
		// 创建 download文件
		_, err := h.diskStore.CreateFile(ctx, h.diskStore.GetPath(storage.GetDownloadRaw(task.TaskId)))
		if err != nil {
			return errors.Wrap(err, "failed to create download file")
		}
	}
	return nil
}

func (h *hybridStorage) GetDownloadPath(rawFunc *store.Raw) string {
	return h.diskStore.GetPath(rawFunc)
}

func (h *hybridStorage) StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error) {
	return h.diskStore.Stat(ctx, storage.GetDownloadRaw(taskId))
}

func (h *hybridStorage) GetAvailSpace(ctx context.Context, raw *store.Raw) (fileutils.Fsize, error) {
	return fileutils.Fsize(100000), nil
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

func getHardLinkRaw(taskId string) *store.Raw {
	raw := storage.GetDownloadRaw(taskId)
	raw.Key = raw.Key + ".hard"
	return raw
}

func init() {
	storage.Register(&hybridBuilder{})
}
