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

//go:generate mockgen -destination ./mock/mock_storage_manager.go -package mock d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage Manager

package storage

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

var (
	// managerMap is a map from name to storageManager builder.
	managerMap = make(map[string]Builder)
)

var (
	ErrTaskNotPersisted = errors.New("task is not persisted")
)

type Manager interface {

	// ResetRepo reset the storage of task
	ResetRepo(task *task.SeedTask) error

	// StatDownloadFile stat download file info, if task file is not exist on storage, return errTaskNotPersisted
	StatDownloadFile(taskID string) (*storedriver.StorageInfo, error)

	// WriteDownloadFile write data to download file
	WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error

	// ReadDownloadFile return reader of download file
	ReadDownloadFile(taskID string) (io.ReadCloser, error)

	// ReadFileMetadata return meta data of download file
	ReadFileMetadata(taskID string) (*FileMetadata, error)

	// WriteFileMetadata write file meta to storage
	WriteFileMetadata(taskID string, meta *FileMetadata) error

	// WritePieceMetaRecords write piece meta records to storage
	WritePieceMetaRecords(taskID string, metaRecords []*PieceMetaRecord) error

	// AppendPieceMetadata append piece meta data to storage
	AppendPieceMetadata(taskID string, metaRecord *PieceMetaRecord) error

	// ReadPieceMetaRecords read piece meta records from storage
	ReadPieceMetaRecords(taskID string) ([]*PieceMetaRecord, error)

	// DeleteTask delete task from storage
	DeleteTask(taskID string) error

	// TryFreeSpace checks if there is enough space for the file, return true while we are sure that there is enough space.
	TryFreeSpace(fileLength int64) (bool, error)
}

// FileMetadata meta data of task
type FileMetadata struct {
	TaskID           string            `json:"taskID"`
	TaskURL          string            `json:"taskURL"`
	PieceSize        int32             `json:"pieceSize"`
	SourceFileLen    int64             `json:"sourceFileLen"`
	AccessTime       int64             `json:"accessTime"`
	Interval         int64             `json:"interval"`
	CdnFileLength    int64             `json:"cdnFileLength"`
	Digest           string            `json:"digest"`
	SourceRealDigest string            `json:"sourceRealDigest"`
	Tag              string            `json:"tag"`
	ExpireInfo       map[string]string `json:"expireInfo"`
	Finish           bool              `json:"finish"`
	Success          bool              `json:"success"`
	TotalPieceCount  int32             `json:"totalPieceCount"`
	PieceMd5Sign     string            `json:"pieceMd5Sign"`
	Range            string            `json:"range"`
	Filter           string            `json:"filter"`
}

// PieceMetaRecord meta data of piece
type PieceMetaRecord struct {
	// piece Num start from 0
	PieceNum uint32 `json:"pieceNum"`
	// 存储到存储介质的真实长度
	PieceLen uint32 `json:"pieceLen"`
	// for transported piece content，不是origin source 的 md5，是真是存储到存储介质后的md5（为了读取数据文件时方便校验完整性）
	Md5 string `json:"md5"`
	// 下载存储到磁盘的range，不是origin source的range.提供给客户端发送下载请求,for transported piece content
	Range *rangeutils.Range `json:"range"`
	//  piece's real offset in the file
	OriginRange *rangeutils.Range `json:"originRange"`
	// 0: PlainUnspecified
	PieceStyle int32 `json:"pieceStyle"`
	// total time(millisecond) consumed
	DownloadCost      uint64 `json:"downloadCost"`
	BeginDownloadTime uint64 `json:"beginDownloadTime"`
	EndDownloadTime   uint64 `json:"endDownloadTime"`
}

const fieldSeparator = ":"

func (record PieceMetaRecord) String() string {
	return fmt.Sprint(record.PieceNum, fieldSeparator, record.PieceLen, fieldSeparator, record.Md5, fieldSeparator, record.Range, fieldSeparator,
		record.OriginRange, fieldSeparator, record.PieceStyle, fieldSeparator, record.DownloadCost, fieldSeparator, record.BeginDownloadTime, fieldSeparator,
		record.EndDownloadTime)
}

func ParsePieceMetaRecord(value string) (record *PieceMetaRecord, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.Errorf("%v", msg)
		}
	}()
	fields := strings.Split(value, fieldSeparator)
	pieceNum, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceNum: %s", fields[0])
	}
	pieceLen, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceLen: %s", fields[1])
	}
	md5 := fields[2]
	pieceRange, err := rangeutils.GetRange(fields[3])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid piece range: %s", fields[3])
	}
	originRange, err := rangeutils.GetRange(fields[4])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid origin range: %s", fields[4])
	}
	pieceStyle, err := strconv.ParseInt(fields[5], 10, 8)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceStyle: %s", fields[5])
	}
	downloadCost, err := strconv.ParseUint(fields[6], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid download cost: %s", fields[6])
	}
	beginDownloadTime, err := strconv.ParseUint(fields[7], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid begin download time: %s", fields[7])
	}
	endDownloadTime, err := strconv.ParseUint(fields[8], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid end download time: %s", fields[8])
	}
	return &PieceMetaRecord{
		PieceNum:          uint32(pieceNum),
		PieceLen:          uint32(pieceLen),
		Md5:               md5,
		Range:             pieceRange,
		OriginRange:       originRange,
		PieceStyle:        int32(pieceStyle),
		DownloadCost:      downloadCost,
		BeginDownloadTime: beginDownloadTime,
		EndDownloadTime:   endDownloadTime,
	}, nil
}

// Builder creates a balancer.
type Builder interface {
	// Build creates a new balancer with the ClientConn.
	Build(cfg Config, taskManager task.Manager) (Manager, error)
	// Name returns the name of balancers built by this builder.
	// It will be used to pick balancers (for example in service config).
	Name() string
	// Validate driver configs
	Validate(map[string]*DriverConfig) []error
	// DefaultDriverConfigs default driver config
	DefaultDriverConfigs() map[string]*DriverConfig
}

// Register defines an interface to register a storage manager builder.
// All storage managers should call this function to register its builder to the storage manager factory.
func Register(builder Builder) {
	managerMap[strings.ToLower(builder.Name())] = builder
}

// Get return a storage manager from manager with specified name.
func Get(name string) Builder {
	if b, ok := managerMap[strings.ToLower(name)]; ok {
		return b
	}
	return nil
}

func NewManager(config Config, taskManager task.Manager) (Manager, error) {
	// Initialize storage manager
	storageManagerBuilder := Get(config.StorageMode)
	if storageManagerBuilder == nil {
		return nil, fmt.Errorf("can not find storage manager mode %s", config.StorageMode)
	}
	return storageManagerBuilder.Build(config, taskManager)
}
