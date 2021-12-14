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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
)

type Cleaner interface {
	GC(storagePattern string, force bool) ([]string, error)
}

type cleaner struct {
	config         *DriverGCConfig
	driver         storedriver.Driver
	taskManager    task.Manager
	storageManager Manager
}

func NewStorageCleaner(config *DriverGCConfig, driver storedriver.Driver, storageManager Manager, taskManager task.Manager) (Cleaner, error) {
	return &cleaner{
		config:         config,
		driver:         driver,
		taskManager:    taskManager,
		storageManager: storageManager,
	}, nil
}

func (cleaner *cleaner) GC(storagePattern string, force bool) ([]string, error) {
	freeSpace, err := cleaner.driver.GetFreeSpace()
	if err != nil {
		if os.IsNotExist(err) {
			err = cleaner.driver.CreateBaseDir()
			if err != nil {
				return nil, err
			}
			freeSpace, err = cleaner.driver.GetFreeSpace()
			if err != nil {
				return nil, fmt.Errorf("get free space: %v", err)
			}
		} else {
			return nil, fmt.Errorf("get free space: %v", err)
		}
	}
	fullGC := force
	if !fullGC {
		if freeSpace > cleaner.config.YoungGCThreshold {
			return nil, nil
		}
		if freeSpace <= cleaner.config.FullGCThreshold {
			fullGC = true
		}
	}

	logger.GcLogger.With("type", storagePattern).Debugf("storage is insufficient, start to exec gc with fullGC: %t", fullGC)

	gapTasks := treemap.NewWith(godsutils.Int64Comparator)
	intervalTasks := treemap.NewWith(godsutils.Int64Comparator)

	// walkTaskIds is used to avoid processing multiple times for the same taskId
	// which is extracted from file name.
	walkTaskIds := make(map[string]bool)
	var gcTaskIDs []string
	walkFn := func(path string, info os.FileInfo, err error) error {
		logger.GcLogger.With("type", storagePattern).Debugf("start to walk path(%s)", path)

		if err != nil {
			logger.GcLogger.With("type", storagePattern).Errorf("failed to access path(%s): %v", path, err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		taskID := strings.Split(info.Name(), ".")[0]
		// If the taskID has been handled, and no need to do that again.
		if walkTaskIds[taskID] {
			return nil
		}
		walkTaskIds[taskID] = true

		// we should return directly when success to get info which means it is being used
		if _, exist := cleaner.taskManager.Exist(taskID); exist {
			return nil
		}

		// add taskID to gcTaskIds slice directly when fullGC equals true.
		if fullGC {
			gcTaskIDs = append(gcTaskIDs, taskID)
			return nil
		}

		metadata, err := cleaner.storageManager.ReadFileMetadata(taskID)
		if err != nil || metadata == nil {
			logger.GcLogger.With("type", storagePattern).Debugf("taskID: %s, failed to get metadata: %v", taskID, err)
			gcTaskIDs = append(gcTaskIDs, taskID)
			return nil
		}
		// put taskID into gapTasks or intervalTasks which will sort by some rules
		if err := cleaner.sortInert(gapTasks, intervalTasks, metadata); err != nil {
			logger.GcLogger.With("type", storagePattern).Errorf("failed to parse inert metadata(%#v): %v", metadata, err)
		}

		return nil
	}

	if err := cleaner.driver.Walk(&storedriver.Raw{
		WalkFn: walkFn,
	}); err != nil {
		return nil, err
	}

	if !fullGC {
		gcTaskIDs = append(gcTaskIDs, cleaner.getGCTasks(gapTasks, intervalTasks)...)
	}

	return gcTaskIDs, nil
}

func (cleaner *cleaner) sortInert(gapTasks, intervalTasks *treemap.Map, metadata *FileMetadata) error {
	gap := timeutils.CurrentTimeMillis() - metadata.AccessTime

	if metadata.Interval > 0 &&
		gap <= metadata.Interval+(int64(cleaner.config.IntervalThreshold.Seconds())*int64(time.Millisecond)) {
		info, err := cleaner.storageManager.StatDownloadFile(metadata.TaskID)
		if err != nil {
			return err
		}

		v, found := intervalTasks.Get(info.Size)
		if !found {
			v = make([]string, 0)
		}
		tasks := v.([]string)
		tasks = append(tasks, metadata.TaskID)
		intervalTasks.Put(info.Size, tasks)
		return nil
	}

	v, found := gapTasks.Get(gap)
	if !found {
		v = make([]string, 0)
	}
	tasks := v.([]string)
	tasks = append(tasks, metadata.TaskID)
	gapTasks.Put(gap, tasks)
	return nil
}

func (cleaner *cleaner) getGCTasks(gapTasks, intervalTasks *treemap.Map) []string {
	var gcTasks = make([]string, 0)

	for _, v := range gapTasks.Values() {
		if taskIDs, ok := v.([]string); ok {
			gcTasks = append(gcTasks, taskIDs...)
		}
	}

	for _, v := range intervalTasks.Values() {
		if taskIDs, ok := v.([]string); ok {
			gcTasks = append(gcTasks, taskIDs...)
		}
	}

	gcLen := (len(gcTasks)*cleaner.config.CleanRatio + 9) / 10
	return gcTasks[0:gcLen]
}
