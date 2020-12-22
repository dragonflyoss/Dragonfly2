/*
 * Copyright The Dragonfly Authors.
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

package localcdn

import (
	"context"
	"path"

	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
)
//get download path
var getDownloadRawFunc = getDownloadRaw
//get task metaData path
var getTaskMetaDataRawFunc = getTaskMetaDataRaw
//get piece metaData path
var getPieceMetaDataRawFunc = getPieceMetaDataRaw
//get home path
var getHomeRawFunc = getHomeRaw

func getDownloadKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID)
}

func getTaskMetaDataKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID+".meta")
}

func getPieceMetaDataKey(taskID string) string {
	return path.Join(getParentKey(taskID), taskID+".piece")
}

func getParentKey(taskID string) string {
	return stringutils.SubString(taskID, 0, 3)
}

func getDownloadRaw(taskID string) *store.Raw {
	return &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getDownloadKey(taskID),
	}
}

func getTaskMetaDataRaw(taskID string) *store.Raw {
	return &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getTaskMetaDataKey(taskID),
		Trunc:  true,
	}
}

func getPieceMetaDataRaw(taskID string) *store.Raw {
	return &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getPieceMetaDataKey(taskID),
		Trunc:  true,
	}
}

func getParentRaw(taskID string) *store.Raw {
	return &store.Raw{
		Bucket: config.DownloadHome,
		Key:    getParentKey(taskID),
	}
}

func getHomeRaw() *store.Raw {
	return &store.Raw{
		Bucket: config.DownloadHome,
	}
}

func deleteTaskFiles(ctx context.Context, cacheStore *store.Store, taskID string) error {
	// delete task meta data
	if err := cacheStore.Remove(ctx, getTaskMetaDataRaw(taskID)); err != nil &&
		!store.IsKeyNotFound(err) {
		return err
	}
	// delete piece meta data
	if err := cacheStore.Remove(ctx, getPieceMetaDataRaw(taskID)); err != nil &&
		!store.IsKeyNotFound(err) {
		return err
	}
	// delete task file data
	if err := cacheStore.Remove(ctx, getDownloadRaw(taskID)); err != nil &&
		!store.IsKeyNotFound(err) {
		return err
	}
	// try to clean the parent bucket
	if err := cacheStore.Remove(ctx, getParentRaw(taskID)); err != nil &&
		!store.IsKeyNotFound(err) {
		logrus.Warnf("taskID:%s failed remove parent bucket:%v", taskID, err)
	}
	return nil
}
