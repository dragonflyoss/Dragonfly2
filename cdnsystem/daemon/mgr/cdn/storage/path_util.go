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
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"path"
)

const (
	// RepoHome is the directory where store data
	RepoHome = "repo"
	// DownloadHome is the parent directory where the downloaded files are stored
	// which is a relative path.
	DownloadHome = "download"

	UploadHome = "upload"

	ShmHome = "/private/var/vm/dragonfly/"
)

func getDownloadKey(taskId string) string {
	return path.Join(getParentKey(taskId), taskId)
}

func getTaskMetaDataKey(taskId string) string {
	return path.Join(getParentKey(taskId), taskId+".meta")
}

func getPieceMetaDataKey(TaskId string) string {
	return path.Join(getParentKey(TaskId), TaskId+".piece")
}

func getParentKey(taskId string) string {
	return stringutils.SubString(taskId, 0, 3)
}

func GetDownloadRaw(taskId string) *store.Raw {
	return &store.Raw{
		Bucket: DownloadHome,
		Key:    getDownloadKey(taskId),
	}
}

func GetUploadRaw(taskId string) *store.Raw {
	return &store.Raw {
		Bucket: UploadHome,
		Key:    getDownloadKey(taskId),
	}
}

func GetTaskMetaDataRaw(taskId string) *store.Raw {
	return &store.Raw{
		Bucket: DownloadHome,
		Key:    getTaskMetaDataKey(taskId),
		Trunc:  true,
	}
}

func GetPieceMetaDataRaw(taskId string) *store.Raw {
	return &store.Raw{
		Bucket: DownloadHome,
		Key:    getPieceMetaDataKey(taskId),
	}
}

func GetParentRaw(taskId string) *store.Raw {
	return &store.Raw{
		Bucket: DownloadHome,
		Key:    getParentKey(taskId),
	}
}

func GetDownloadHomeRaw() *store.Raw {
	return &store.Raw{
		Bucket: DownloadHome,
	}
}
