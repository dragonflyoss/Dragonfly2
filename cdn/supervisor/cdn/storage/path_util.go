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
	"path"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

const (
	// DownloadHome is the parent directory where the downloaded files are stored
	// which is a relative path.
	DownloadHome = "download"

	UploadHome = "upload"
)

func getDownloadKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID)
}

func getTaskMetadataKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID+".meta")
}

func getPieceMetadataKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID+".piece")
}

func getParentKey(taskID string) string {
	return stringutils.SubString(taskID, 0, 3)
}

func GetDownloadRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
		Key:    getDownloadKey(taskID),
	}
}

func GetUploadRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: UploadHome,
		Key:    getDownloadKey(taskID),
	}
}

func GetTaskMetadataRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
		Key:    getTaskMetadataKey(taskID),
		Trunc:  true,
	}
}

func GetPieceMetadataRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
		Key:    getPieceMetadataKey(taskID),
	}
}

func GetAppendPieceMetadataRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
		Key:    getPieceMetadataKey(taskID),
		Append: true,
	}
}

func GetParentRaw(taskID string) *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
		Key:    getParentKey(taskID),
	}
}

func GetDownloadHomeRaw() *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: DownloadHome,
	}
}

func GetUploadHomeRaw() *storedriver.Raw {
	return &storedriver.Raw{
		Bucket: UploadHome,
	}
}
