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
	"fmt"
	"io"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

var (
	builderMap     = make(map[string]Builder)
	defaultStorage = "disk"
)

func Register(b Builder) {
	builderMap[strings.ToLower(b.Name())] = b
}

func getBuilder(name string, defaultIfAbsent bool) Builder {
	if b, ok := builderMap[strings.ToLower(name)]; ok {
		return b
	}
	if stringutils.IsBlank(name) && defaultIfAbsent {
		return builderMap[defaultStorage]
	}
	return nil
}

// Builder creates a storage
type Builder interface {
	Build(cfg *config.Config) (Manager, error)

	Name() string
}

type BuildOptions interface {
}

// fileMetaData
type FileMetaData struct {
	TaskID        string            `json:"taskId"`
	TaskURL       string            `json:"taskUrl"`
	PieceSize     int32             `json:"pieceSize"`
	SourceFileLen int64             `json:"sourceFileLen"`
	AccessTime    int64             `json:"accessTime"`
	Interval      int64             `json:"interval"`
	CdnFileLength int64             `json:"cdnFileLength"`
	SourceRealMd5 string            `json:"sourceRealMd5"`
	PieceMd5Sign  string            `json:"pieceMd5Sign"`
	ExpireInfo    map[string]string `json:"expireInfo"`
	Finish        bool              `json:"finish"`
	Success         bool              `json:"success"`
	TotalPieceCount int32             `json:"totalPieceCount"`
	//PieceMetaDataSign string            `json:"pieceMetaDataSign"`
}

const fieldSeparator = ":"

// pieceMetaRecord
type PieceMetaRecord struct {
	PieceNum    int32             `json:"pieceNum"`    // piece Num start from 0
	PieceLen    int32             `json:"pieceLen"`    // 存储到存储介质的真实长度
	Md5         string            `json:"md5"`         // for transported piece content，不是origin source 的 md5，是真是存储到存储介质后的md5（为了读取数据文件时方便校验完整性）
	Range       *rangeutils.Range `json:"range"`       // 下载存储到磁盘的range，不是origin source的range.提供给客户端发送下载请求,for transported piece content
	OriginRange *rangeutils.Range `json:"originRange"` //  piece's real offset in the file
	PieceStyle  types.PieceFormat `json:"pieceStyle"`  // 1: PlainUnspecified
}

func (record PieceMetaRecord) String() string {
	return fmt.Sprintf("%d%s%d%s%s%s%s%s%s%s%d", record.PieceNum, fieldSeparator, record.PieceLen, fieldSeparator, record.Md5, fieldSeparator, record.Range,
		fieldSeparator, record.OriginRange, fieldSeparator, record.PieceStyle)
}

func ParsePieceMetaRecord(value string) (record *PieceMetaRecord, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.Errorf("%v", msg)
		}
	}()
	fields := strings.Split(value, fieldSeparator)
	pieceNum, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceNum:%s", fields[0])
	}
	pieceLen, err := strconv.ParseInt(fields[1], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceLen:%s", fields[1])
	}
	md5 := fields[2]
	pieceRange, err := rangeutils.ParseRange(fields[3])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid piece range:%s", fields[3])
	}
	originRange, err := rangeutils.ParseRange(fields[4])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid origin range:%s", fields[4])
	}
	if err != nil {
		return nil, errors.Wrapf(err, "invalid offset:%s", fields[4])
	}
	pieceStyle, err := strconv.ParseInt(fields[5], 10, 8)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceStyle:%s", fields[5])
	}
	return &PieceMetaRecord{
		PieceNum:    int32(pieceNum),
		PieceLen:    int32(pieceLen),
		Md5:         md5,
		Range:       pieceRange,
		OriginRange: originRange,
		PieceStyle:  types.PieceFormat(pieceStyle),
	}, nil
}

func NewManager(cfg *config.Config) (Manager, error) {
	sb := getBuilder(cfg.StoragePattern, true)
	if sb == nil {
		return nil, fmt.Errorf("could not get storage for pattern: %s", cfg.StoragePattern)
	}
	logger.Debugf("storage pattern is %s", sb.Name())
	return sb.Build(cfg)
}

type Manager interface {
	ResetRepo(context.Context, *types.SeedTask) error

	StatDownloadFile(context.Context, string) (*storedriver.StorageInfo, error)

	WriteDownloadFile(context.Context, string, int64, int64, *bytes.Buffer) error

	ReadDownloadFile(context.Context, string) (io.ReadCloser, error)

	CreateUploadLink(context.Context, string) error

	ReadFileMetaData(context.Context, string) (*FileMetaData, error)

	WriteFileMetaData(context.Context, string, *FileMetaData) error

	WritePieceMetaRecords(context.Context, string, []*PieceMetaRecord) error

	AppendPieceMetaData(context.Context, string, *PieceMetaRecord) error

	ReadPieceMetaRecords(context.Context, string) ([]*PieceMetaRecord, error)

	DeleteTask(context.Context, string) error

	SetTaskMgr(mgr.SeedTaskMgr)

	InitializeCleaners()
}
