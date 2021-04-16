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
	TaskId           string            `json:"taskId,omitempty"`
	Url              string            `json:"url,omitempty"`
	TaskUrl          string            `json:"taskUrl,omitempty"`
	SourceFileLength int64             `json:"sourceFileLength,omitempty"`
	CdnFileLength    int64             `json:"cdnFileLength,omitempty"`
	PieceSize        int32             `json:"pieceSize,omitempty"`
	Header           map[string]string `json:"header,omitempty"`
	CdnStatus        string            `json:"cdnStatus,omitempty"`
	PieceTotal       int32             `json:"pieceTotal,omitempty"`
	RequestMd5       string            `json:"requestMd5,omitempty"`
	SourceRealMd5    string            `json:"sourceRealMd5,omitempty"`
	PieceMd5Sign     string            `json:"pieceMd5Sign,omitempty"`
}

// IsSuccess determines that whether the CDNStatus is success.
func (task *SeedTask) IsSuccess() bool {
	return task.CdnStatus == TaskInfoCdnStatusSuccess
}

// IsFrozen
func (task *SeedTask) IsFrozen() bool {
	return task.CdnStatus == TaskInfoCdnStatusFailed ||
		task.CdnStatus == TaskInfoCdnStatusWaiting ||
		task.CdnStatus == TaskInfoCdnStatusSourceError
}

// IsWait
func (task *SeedTask) IsWait() bool {
	return task.CdnStatus == TaskInfoCdnStatusWaiting
}

// IsError
func (task *SeedTask) IsError() bool {
	return task.CdnStatus == TaskInfoCdnStatusFailed ||
		task.CdnStatus == TaskInfoCdnStatusSourceError
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
