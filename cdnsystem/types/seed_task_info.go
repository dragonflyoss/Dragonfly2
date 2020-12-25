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

type UrlMeta struct {
	// md5 of content downloaded from url
	Md5 string
	// downloading range for single file
	Range string
}

type SeedTask struct {
	TaskID           string
	Url              string
	SourceFileLength int64
	CdnFileLength    int64
	PieceSize        int32
	Headers          map[string]string
	CdnStatus        string
	PieceTotal       int32
	RequestMd5       string
	SourceRealMd5    string
}

const (

	// TaskInfoCdnStatusWAITING captures enum value "WAITING"
	TaskInfoCdnStatusWAITING string = "WAITING"

	// TaskInfoCdnStatusRUNNING captures enum value "RUNNING"
	TaskInfoCdnStatusRUNNING string = "RUNNING"

	// TaskInfoCdnStatusFAILED captures enum value "FAILED"
	TaskInfoCdnStatusFAILED string = "FAILED"

	// TaskInfoCdnStatusSUCCESS captures enum value "SUCCESS"
	TaskInfoCdnStatusSUCCESS string = "SUCCESS"

	// TaskInfoCdnStatusSOURCEERROR captures enum value "SOURCE_ERROR"
	TaskInfoCdnStatusSOURCEERROR string = "SOURCE_ERROR"
)
