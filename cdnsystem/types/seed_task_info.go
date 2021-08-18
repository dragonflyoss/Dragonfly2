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

package types

type SeedTask struct {
	TaskID           string            `json:"taskId,omitempty"`
	URL              string            `json:"url,omitempty"`
	TaskURL          string            `json:"taskUrl,omitempty"`
	SourceFileLength int64             `json:"sourceFileLength,omitempty"`
	CdnFileLength    int64             `json:"cdnFileLength,omitempty"`
	PieceSize        int32             `json:"pieceSize,omitempty"`
	Header           map[string]string `json:"header,omitempty"`
	CdnStatus        string            `json:"cdnStatus,omitempty"`
	PieceTotal       int32             `json:"pieceTotal,omitempty"`
	RequestDigest    string            `json:"requestDigest,omitempty"`
	SourceRealDigest string            `json:"sourceRealDigest,omitempty"`
	PieceMd5Sign     string            `json:"pieceMd5Sign,omitempty"`
}

// IsSuccess determines that whether the CDNStatus is success.
func (task *SeedTask) IsSuccess() bool {
	return task.CdnStatus == TaskInfoCdnStatusSuccess
}

// IsFrozen if task status is frozen
func (task *SeedTask) IsFrozen() bool {
	return task.CdnStatus == TaskInfoCdnStatusFailed || task.CdnStatus == TaskInfoCdnStatusWaiting || task.CdnStatus == TaskInfoCdnStatusSourceError
}

// IsWait if task status is wait
func (task *SeedTask) IsWait() bool {
	return task.CdnStatus == TaskInfoCdnStatusWaiting
}

// IsError if task status if fail
func (task *SeedTask) IsError() bool {
	return task.CdnStatus == TaskInfoCdnStatusFailed || task.CdnStatus == TaskInfoCdnStatusSourceError
}

func (task *SeedTask) IsDone() bool {
	return task.CdnStatus == TaskInfoCdnStatusFailed || task.CdnStatus == TaskInfoCdnStatusSuccess || task.CdnStatus == TaskInfoCdnStatusSourceError
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

const (

	// TaskInfoCdnStatusWaiting captures enum value "WAITING"
	TaskInfoCdnStatusWaiting string = "WAITING"

	// TaskInfoCdnStatusRunning captures enum value "RUNNING"
	TaskInfoCdnStatusRunning string = "RUNNING"

	// TaskInfoCdnStatusFailed captures enum value "FAILED"
	TaskInfoCdnStatusFailed string = "FAILED"

	// TaskInfoCdnStatusSuccess captures enum value "SUCCESS"
	TaskInfoCdnStatusSuccess string = "SUCCESS"

	// TaskInfoCdnStatusSourceError captures enum value "SOURCE_ERROR"
	TaskInfoCdnStatusSourceError string = "SOURCE_ERROR"
)
