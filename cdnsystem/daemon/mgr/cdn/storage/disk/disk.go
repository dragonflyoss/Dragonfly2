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
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	underDisk "d7y.io/dragonfly/v2/cdnsystem/store/disk"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
)

const name = "disk"

type diskBuilder struct {
}

func (*diskBuilder) Build() (storage.Storage, error) {
	underStore, _ := underDisk.NewStorage("baseDir: /tmp/cdnsystem/")
	storage := &diskStorage{
		diskStore: underStore,
	}
	return storage, nil
}

func (*diskBuilder) Name() string {
	return name
}

type diskStorage struct {
	diskStore store.StorageDriver
}

func (s *diskStorage) Walk(ctx context.Context, raw *store.Raw) error {
	panic("implement me")
}

func (s *diskStorage) WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64, buf *bytes.Buffer) error {
	raw := storage.GetDownloadRaw(taskId)
	raw.Offset = offset
	raw.Length = len
	return s.diskStore.Put(ctx, raw, buf)
}

func (s *diskStorage) ReadFileMetaDataBytes(ctx context.Context, taskId string) ([]byte, error) {
	return s.diskStore.GetBytes(ctx, storage.GetTaskMetaDataRaw(taskId))
}

func (s *diskStorage) WriteFileMetaDataBytes(ctx context.Context, taskId string, data []byte) error {
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

func (s *diskStorage) GetAvailSpace(ctx context.Context, raw *store.Raw) (fileutils.Fsize, error) {
	return s.diskStore.GetAvailSpace(ctx, raw)
}

func (s *diskStorage) CreateUploadLink(taskId string) error {
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
