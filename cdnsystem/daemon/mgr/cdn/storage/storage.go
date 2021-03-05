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

package storage

import (
	"bytes"
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"io"
	"strings"
)

var (
	m              = make(map[string]Builder)
	defaultStorage = "disk"
)

func Register(b Builder) {
	m[strings.ToLower(b.Name())] = b
}

func Get(name string, defaultIfAbsent bool) Builder {
	if b, ok := m[strings.ToLower(name)]; ok {
		return b
	}
	if defaultIfAbsent {
		return m[defaultStorage]
	}
	return nil
}

// Builder creates a storage
type Builder interface {

	Build(BuildOptions) (Storage, error)

	Name() string
}

type BuildOptions interface {

}

type Storage interface {

	ResetRepo(ctx context.Context, task *types.SeedTask) error

	// Stat determines whether the data exists based on raw information.
	// If that, and return some info that in the form of struct StorageInfo.
	// If not, return the ErrFileNotExist.
	StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error)

	// GetAvailSpace returns the available disk space in B.
	GetAvailSpace(ctx context.Context, raw *store.Raw) (fileutils.Fsize, error)

	CreateUploadLink(taskId string) error

	ReadFileMetaDataBytes(ctx context.Context, taskId string) ([]byte, error)

	WriteFileMetaDataBytes(ctx context.Context, taskId string, data []byte) error

	AppendPieceMetaDataBytes(ctx context.Context, taskId string, bytes []byte) error

	ReadPieceMetaBytes(ctx context.Context, taskId string) ([]byte, error)

	ReadDownloadFile(ctx context.Context, taskId string) (io.Reader, error)

	DeleteTask(ctx context.Context, taskId string) error

	WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64, buf *bytes.Buffer) error

	Walk(ctx context.Context, raw *store.Raw) error

	SetTaskMgr(mgr.SeedTaskMgr)

	Gc(ctx context.Context)
}
