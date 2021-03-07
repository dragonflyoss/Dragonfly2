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
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
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
	if stringutils.IsBlank(name) && defaultIfAbsent {
		return m[defaultStorage]
	}
	return nil
}

// Builder creates a storage
type Builder interface {

	Build() (StorageMgr, error)

	Name() string
}

type BuildOptions interface {

}

// fileMetaData
type FileMetaData struct {
	TaskId            string            `json:"taskId"`
	TaskURL           string            `json:"taskUrl"`
	PieceSize         int32             `json:"pieceSize"`
	SourceFileLen     int64             `json:"sourceFileLen"`
	AccessTime        int64             `json:"accessTime"`
	Interval          int64             `json:"interval"`
	CdnFileLength     int64             `json:"cdnFileLength"`
	SourceRealMd5     string            `json:"sourceRealMd5"`
	PieceMd5Sign      string            `json:"pieceMd5Sign"`
	//PieceMetaDataSign string            `json:"pieceMetaDataSign"`
	ExpireInfo        map[string]string `json:"expireInfo"`
	Finish            bool              `json:"finish"`
	Success           bool              `json:"success"`
	TotalPieceCount   int32             `json:"totalPieceCount"`
}

// pieceMetaRecord
type PieceMetaRecord struct {
	PieceNum   int32             `json:"pieceNum"`   // piece Num start from 0
	PieceLen   int32             `json:"pieceLen"`   // 下载存储的真实长度
	Md5        string            `json:"md5"`        // piece content md5
	Range      string            `json:"range"`      // 下载存储到磁盘的range，不一定是origin source的range
	Offset     uint64            `json:"offset"`     // offset
	PieceStyle types.PieceFormat `json:"pieceStyle"` // 1: PlainUnspecified
}

type StorageMgr interface {

	ResetRepo(ctx context.Context, task *types.SeedTask) error

	StatDownloadFile(ctx context.Context, taskId string) (*store.StorageInfo, error)

	WriteDownloadFile(ctx context.Context, taskId string, offset int64, len int64, buf *bytes.Buffer) error

	ReadDownloadFile(ctx context.Context, taskId string) (io.Reader, error)

	CreateUploadLink(ctx context.Context, taskId string) error

	ReadFileMetaData(ctx context.Context, taskId string) (*FileMetaData, error)

	WriteFileMetaData(ctx context.Context, taskId string, data *FileMetaData) error

	AppendPieceMetaData(ctx context.Context, taskId string, pieceRecord *PieceMetaRecord) error

	ReadPieceMetaRecords(ctx context.Context, taskId string) ([]*PieceMetaRecord, error)

	DeleteTask(ctx context.Context, taskId string) error

	SetTaskMgr(mgr.SeedTaskMgr)

	Gc(ctx context.Context)
}