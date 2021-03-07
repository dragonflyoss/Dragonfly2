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
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
	"github.com/pkg/errors"
	"os"
	"strings"
	"time"
)

type gcConfig struct {
	YoungGCThreshold  fileutils.Fsize
	FullGCThreshold   fileutils.Fsize
	cleanRatio        int
	IntervalThreshold time.Duration
}

type StorageCleaner struct {
	cfg        *gcConfig
	store      store.StorageDriver
	storageMgr StorageMgr
	taskMgr    mgr.SeedTaskMgr
}

func NewStorageCleaner(gcConfig *gcConfig, store store.StorageDriver, storageMgr StorageMgr,
	taskMgr mgr.SeedTaskMgr) *StorageCleaner {
	return &StorageCleaner{
		cfg:        gcConfig,
		store:      store,
		storageMgr: storageMgr,
		taskMgr:    taskMgr,
	}
}

func (cleaner *StorageCleaner) Gc(ctx context.Context, force bool) ([]string, error) {
	freeSpace, err := cleaner.store.GetAvailSpace(ctx)
	if err != nil {
		if cdnerrors.IsKeyNotFound(err) {
			err = cleaner.store.CreateDir(ctx, cleaner.store.GetHomePath(ctx))
			if err != nil {
				return nil, err
			}
			freeSpace, err = cleaner.store.GetAvailSpace(ctx)
		} else {
			return nil, errors.Wrapf(err, "failed to get avail space")
		}
	}
	fullGC := force
	if !fullGC {
		if freeSpace > cleaner.cfg.YoungGCThreshold {
			return nil, nil
		}
		if freeSpace <= cleaner.cfg.FullGCThreshold {
			fullGC = true
		}
	}

	logger.GcLogger.Debugf("start to exec gc with fullGC: %t", fullGC)

	gapTasks := treemap.NewWith(godsutils.Int64Comparator)
	intervalTasks := treemap.NewWith(godsutils.Int64Comparator)

	// walkTaskIds is used to avoid processing multiple times for the same taskId
	// which is extracted from file name.
	walkTaskIds := make(map[string]bool)
	var gcTaskIDs []string
	walkFn := func(path string, info os.FileInfo, err error) error {
		logger.GcLogger.Debugf("start to walk path(%s)", path)

		if err != nil {
			logger.GcLogger.Errorf("failed to access path(%s): %v", path, err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		taskId := strings.Split(info.Name(), ".")[0]

		// If the taskId has been handled, and no need to do that again.
		if walkTaskIds[taskId] {
			return nil
		}
		walkTaskIds[taskId] = true

		// we should return directly when we success to get info which means it is being used
		if _, err := cleaner.taskMgr.Get(ctx, taskId); err == nil || !cdnerrors.IsDataNotFound(err) {
			if err != nil {
				logger.GcLogger.Errorf("failed to get taskId(%s): %v", taskId, err)
			}
			return nil
		}

		// add taskId to gcTaskIds slice directly when fullGC equals true.
		if fullGC {
			gcTaskIDs = append(gcTaskIDs, taskId)
			return nil
		}

		metaData, err := cleaner.storageMgr.ReadFileMetaData(ctx, taskId)
		if err != nil || metaData == nil {
			logger.GcLogger.Debugf("taskId: %s, failed to get metadata: %v", taskId, err)
			gcTaskIDs = append(gcTaskIDs, taskId)
			return nil
		}
		// put taskId into gapTasks or intervalTasks which will sort by some rules
		if err := cleaner.sortInert(ctx, gapTasks, intervalTasks, metaData); err != nil {
			logger.GcLogger.Errorf("failed to parse inert metaData(%+v): %v", metaData, err)
		}

		return nil
	}

	if err := cleaner.store.Walk(ctx, &store.Raw{
		WalkFn: walkFn,
	}); err != nil {
		return nil, err
	}

	if !fullGC {
		gcTaskIDs = append(gcTaskIDs, getGCTasks(gapTasks, intervalTasks)...)
	}

	return gcTaskIDs, nil
}

func (cleaner *StorageCleaner) sortInert(ctx context.Context, gapTasks, intervalTasks *treemap.Map,
	metaData *FileMetaData) error {
	gap := timeutils.CurrentTimeMillis() - metaData.AccessTime

	if metaData.Interval > 0 &&
		gap <= metaData.Interval+(int64(cleaner.cfg.IntervalThreshold.Seconds())*int64(time.Millisecond)) {
		info, err := cleaner.storageMgr.StatDownloadFile(ctx, metaData.TaskId)
		if err != nil {
			return err
		}

		v, found := intervalTasks.Get(info.Size)
		if !found {
			v = make([]string, 0)
		}
		tasks := v.([]string)
		tasks = append(tasks, metaData.TaskId)
		intervalTasks.Put(info.Size, tasks)
		return nil
	}

	v, found := gapTasks.Get(gap)
	if !found {
		v = make([]string, 0)
	}
	tasks := v.([]string)
	tasks = append(tasks, metaData.TaskId)
	gapTasks.Put(gap, tasks)
	return nil
}

func getGCTasks(gapTasks, intervalTasks *treemap.Map) []string {
	var gcTasks = make([]string, 0)

	for _, v := range gapTasks.Values() {
		if taskIds, ok := v.([]string); ok {
			gcTasks = append(gcTasks, taskIds...)
		}
	}

	for _, v := range intervalTasks.Values() {
		if taskIds, ok := v.([]string); ok {
			gcTasks = append(gcTasks, taskIds...)
		}
	}

	return gcTasks
}
