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

package task

import (
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type SeedTask struct {
	// ID of the task
	ID string `json:"ID,omitempty"`

	// RawURL is the resource's URL which user uses dfget to download. The location of URL can be anywhere, LAN or WAN.
	// For image distribution, this is image layer's URL in image registry.
	// The resource url is provided by dfget command line parameter.
	RawURL string `json:"rawURL,omitempty"`

	// TaskURL is generated from rawURL. rawURL may contain some queries or parameter, dfget will filter some queries via
	// --filter parameter of dfget. The usage of it is that different rawURL may generate the same taskID.
	TaskURL string `json:"taskURL,omitempty"`

	// SourceFileLength is the length of the source file in bytes.
	SourceFileLength int64 `json:"sourceFileLength,omitempty"`

	// CdnFileLength is the length of the file stored on CDN
	CdnFileLength int64 `json:"cdnFileLength,omitempty"`

	// PieceSize is the size of pieces in bytes
	PieceSize int32 `json:"pieceSize,omitempty"`

	// CdnStatus is the status of the created task related to CDN functionality.
	//
	// Enum: [WAITING RUNNING FAILED SUCCESS SOURCE_ERROR]
	CdnStatus string `json:"cdnStatus,omitempty"`

	// TotalPieceCount is the total number of pieces
	TotalPieceCount int32 `json:"totalPieceCount,omitempty"`

	// SourceRealDigest when CDN finishes downloading file/image from the source location,
	// the md5 sum of the source file will be calculated as the value of the SourceRealDigest.
	// And it will be used to compare with RequestDigest value to check whether file is complete.
	SourceRealDigest string `json:"sourceRealDigest,omitempty"`

	// PieceMd5Sign Is the SHA256 signature of all pieces md5 signature
	PieceMd5Sign string `json:"pieceMd5Sign,omitempty"`

	// Digest checks integrity of url content, for example md5:xxx or sha256:yyy
	Digest string `json:"digest,omitempty"`

	// Tag identifies different task for same url, conflict with digest
	Tag string `json:"tag,omitempty"`

	// Range content range for url
	Range string `json:"range,omitempty"`

	// Filter url used to generate task id
	Filter string `json:"filter,omitempty"`

	// Header other url header infos
	Header map[string]string `json:"header,omitempty"`

	// Pieces pieces of task
	Pieces map[uint32]*PieceInfo `json:"-"`

	logger *logger.SugaredLoggerOnWith
}

type PieceInfo struct {
	PieceNum     uint32            `json:"piece_num"`
	PieceMd5     string            `json:"piece_md5"`
	PieceRange   *rangeutils.Range `json:"piece_range"`
	OriginRange  *rangeutils.Range `json:"origin_range"`
	PieceLen     uint32            `json:"piece_len"`
	PieceStyle   base.PieceStyle   `json:"piece_style"`
	DownloadCost uint64            `json:"download_cost"`
}

const (
	UnknownTotalPieceCount = -1
)

func NewSeedTask(taskID string, rawURL string, urlMeta *base.UrlMeta) *SeedTask {
	if urlMeta == nil {
		urlMeta = &base.UrlMeta{}
	}
	return &SeedTask{
		ID:               taskID,
		RawURL:           rawURL,
		TaskURL:          urlutils.FilterURLParam(rawURL, strings.Split(urlMeta.Filter, "&")),
		SourceFileLength: source.UnknownSourceFileLen,
		CdnFileLength:    0,
		PieceSize:        0,
		CdnStatus:        StatusWaiting,
		TotalPieceCount:  UnknownTotalPieceCount,
		SourceRealDigest: "",
		PieceMd5Sign:     "",
		Digest:           urlMeta.Digest,
		Tag:              urlMeta.Tag,
		Range:            urlMeta.Range,
		Filter:           urlMeta.Filter,
		Header:           urlMeta.Header,
		Pieces:           make(map[uint32]*PieceInfo),
		logger:           logger.WithTaskID(taskID),
	}
}

func (task *SeedTask) Clone() *SeedTask {
	cloneTask := new(SeedTask)
	*cloneTask = *task
	if task.Header != nil {
		for key, value := range task.Header {
			cloneTask.Header[key] = value
		}
	}
	if len(task.Pieces) > 0 {
		for pieceNum, piece := range task.Pieces {
			cloneTask.Pieces[pieceNum] = piece
		}
	}
	return cloneTask
}

// IsSuccess determines that whether the CDNStatus is success.
func (task *SeedTask) IsSuccess() bool {
	return task.CdnStatus == StatusSuccess
}

// IsFrozen if task status is frozen
func (task *SeedTask) IsFrozen() bool {
	return task.CdnStatus == StatusFailed ||
		task.CdnStatus == StatusWaiting ||
		task.CdnStatus == StatusSourceError
}

// IsWait if task status is wait
func (task *SeedTask) IsWait() bool {
	return task.CdnStatus == StatusWaiting
}

// IsError if task status if fail
func (task *SeedTask) IsError() bool {
	return task.CdnStatus == StatusFailed || task.CdnStatus == StatusSourceError
}

func (task *SeedTask) IsDone() bool {
	return task.CdnStatus == StatusFailed || task.CdnStatus == StatusSuccess || task.CdnStatus == StatusSourceError
}

func (task *SeedTask) UpdateStatus(cdnStatus string) {
	task.CdnStatus = cdnStatus
}

func (task *SeedTask) UpdateTaskInfo(cdnStatus, realDigest, pieceMd5Sign string, sourceFileLength, cdnFileLength int64) {
	task.CdnStatus = cdnStatus
	task.PieceMd5Sign = pieceMd5Sign
	task.SourceRealDigest = realDigest
	task.SourceFileLength = sourceFileLength
	task.CdnFileLength = cdnFileLength
}

func (task *SeedTask) Log() *logger.SugaredLoggerOnWith {
	if task.logger == nil {
		task.logger = logger.WithTaskID(task.ID)
	}
	return task.logger
}

func (task *SeedTask) StartTrigger() {
	task.CdnStatus = StatusRunning
	task.Pieces = make(map[uint32]*PieceInfo)
}

const (

	// StatusWaiting captures enum value "WAITING"
	StatusWaiting string = "WAITING"

	// StatusRunning captures enum value "RUNNING"
	StatusRunning string = "RUNNING"

	// StatusFailed captures enum value "FAILED"
	StatusFailed string = "FAILED"

	// StatusSuccess captures enum value "SUCCESS"
	StatusSuccess string = "SUCCESS"

	// StatusSourceError captures enum value "SOURCE_ERROR"
	StatusSourceError string = "SOURCE_ERROR"
)

func IsEqual(task1, task2 SeedTask) bool {
	return cmp.Equal(task1, task2, cmpopts.IgnoreFields(SeedTask{}, "Pieces"), cmpopts.IgnoreUnexported(SeedTask{}))
}
