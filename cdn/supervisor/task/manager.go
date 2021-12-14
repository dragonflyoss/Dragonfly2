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

//go:generate mockgen -destination ../mocks/task/mock_task_manager.go -package task d7y.io/dragonfly/v2/cdn/supervisor/task Manager

package task

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdn/gc"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/source"
	pkgsync "d7y.io/dragonfly/v2/pkg/sync"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// Manager as an interface defines all operations against SeedTask.
// A SeedTask will store some meta info about the taskFile, pieces and something else.
// A seedTask corresponds to three files on the disk, which are identified by taskId, the data file meta file piece file
type Manager interface {

	// AddOrUpdate update existing task info for the key if present.
	// Otherwise, it stores and returns the given value.
	// The isUpdate result is true if the value was updated, false if added.
	AddOrUpdate(registerTask *SeedTask) (seedTask *SeedTask, err error)

	// Get returns the task info with specified taskID, or nil if no
	// value is present.
	// The ok result indicates whether value was found in the taskManager.
	Get(taskID string) (seedTask *SeedTask, err error)

	// Update the task info with specified taskID and updateTask
	Update(taskID string, updateTask *SeedTask) (err error)

	// UpdateProgress update the downloaded pieces belonging to the task
	UpdateProgress(taskID string, piece *PieceInfo) (err error)

	// GetProgress returns the downloaded pieces belonging to the task
	GetProgress(taskID string) (map[uint32]*PieceInfo, error)

	// Exist check task existence with specified taskID.
	// returns the task info with specified taskID, or nil if no value is present.
	// The ok result indicates whether value was found in the taskManager.
	Exist(taskID string) (seedTask *SeedTask, ok bool)

	// Delete a task with specified taskID.
	Delete(taskID string)
}

// Ensure that manager implements the Manager and gc.Executor interfaces
var (
	_ Manager     = (*manager)(nil)
	_ gc.Executor = (*manager)(nil)
)

var (
	errTaskNotFound   = errors.New("task is not found")
	errURLUnreachable = errors.New("url is unreachable")
	errTaskIDConflict = errors.New("taskID is conflict")
)

func IsTaskNotFound(err error) bool {
	return errors.Is(err, errTaskNotFound)
}

// manager is an implementation of the interface of Manager.
type manager struct {
	config                  Config
	taskStore               sync.Map
	accessTimeMap           sync.Map
	taskURLUnreachableStore sync.Map
	kmu                     *pkgsync.Krwmutex
}

// NewManager returns a new Manager Object.
func NewManager(config Config) (Manager, error) {
	manager := &manager{
		config: config,
		kmu:    pkgsync.NewKrwmutex(),
	}

	gc.Register("task", config.GCInitialDelay, config.GCMetaInterval, manager)
	return manager, nil
}

func (tm *manager) AddOrUpdate(registerTask *SeedTask) (seedTask *SeedTask, err error) {
	defer func() {
		if err != nil {
			tm.accessTimeMap.Store(registerTask.ID, time.Now())
		}
	}()
	tm.kmu.RLock(registerTask.ID)
	if unreachableTime, ok := tm.getTaskUnreachableTime(registerTask.ID); ok {
		if time.Since(unreachableTime) < tm.config.FailAccessInterval {
			tm.kmu.RUnlock(registerTask.ID)
			// TODO 校验Header
			return nil, errURLUnreachable
		}
		logger.Debugf("delete taskID: %s from unreachable url list", registerTask.ID)
		tm.taskURLUnreachableStore.Delete(registerTask.ID)
	}
	actual, loaded := tm.taskStore.LoadOrStore(registerTask.ID, registerTask)
	seedTask = actual.(*SeedTask)
	if loaded && !IsSame(seedTask, registerTask) {
		tm.kmu.RUnlock(registerTask.ID)
		return nil, errors.Wrapf(errTaskIDConflict, "register task %#v is conflict with exist task %#v", registerTask, seedTask)
	}
	if seedTask.SourceFileLength != source.UnknownSourceFileLen {
		tm.kmu.RUnlock(registerTask.ID)
		return seedTask, nil
	}
	tm.kmu.RUnlock(registerTask.ID)
	tm.kmu.Lock(registerTask.ID)
	defer tm.kmu.Unlock(registerTask.ID)
	if seedTask.SourceFileLength != source.UnknownSourceFileLen {
		return seedTask, nil
	}
	// get sourceContentLength with req.Header
	contentLengthRequest, err := source.NewRequestWithHeader(registerTask.RawURL, registerTask.Header)
	if err != nil {
		return nil, err
	}
	// add range info
	if !stringutils.IsBlank(registerTask.Range) {
		contentLengthRequest.Header.Add(source.Range, registerTask.Range)
	}
	sourceFileLength, err := source.GetContentLength(contentLengthRequest)
	if err != nil {
		registerTask.Log().Errorf("get url (%s) content length failed: %v", registerTask.RawURL, err)
		if source.IsResourceNotReachableError(err) {
			tm.taskURLUnreachableStore.Store(registerTask, time.Now())
		}
		return seedTask, err
	}
	seedTask.SourceFileLength = sourceFileLength
	seedTask.Log().Debugf("success get file content length: %d", sourceFileLength)

	// if success to get the information successfully with the req.Header then update the task.UrlMeta to registerTask.UrlMeta.
	if registerTask.Header != nil {
		seedTask.Header = registerTask.Header
	}

	// calculate piece size and update the PieceSize and PieceTotal
	if registerTask.PieceSize <= 0 {
		pieceSize := util.ComputePieceSize(registerTask.SourceFileLength)
		seedTask.PieceSize = int32(pieceSize)
		seedTask.Log().Debugf("piece size calculate result: %s", unit.ToBytes(int64(pieceSize)))
	}
	return seedTask, nil
}

func (tm *manager) Get(taskID string) (*SeedTask, error) {
	tm.kmu.RLock(taskID)
	defer tm.kmu.RUnlock(taskID)

	// only update access when get task success
	if task, ok := tm.getTask(taskID); ok {
		tm.accessTimeMap.Store(taskID, time.Now())
		return task, nil
	}
	return nil, errTaskNotFound
}

func (tm *manager) Update(taskID string, taskInfo *SeedTask) error {
	tm.kmu.Lock(taskID)
	defer tm.kmu.Unlock(taskID)

	if err := tm.updateTask(taskID, taskInfo); err != nil {
		return err
	}
	// only update access when update task success
	tm.accessTimeMap.Store(taskID, time.Now())
	return nil
}

func (tm *manager) UpdateProgress(taskID string, info *PieceInfo) error {
	tm.kmu.Lock(taskID)
	defer tm.kmu.Unlock(taskID)

	seedTask, ok := tm.getTask(taskID)
	if !ok {
		return errTaskNotFound
	}
	seedTask.Pieces[info.PieceNum] = info
	// only update access when update task success
	tm.accessTimeMap.Store(taskID, time.Now())
	return nil
}

func (tm *manager) GetProgress(taskID string) (map[uint32]*PieceInfo, error) {
	tm.kmu.Lock(taskID)
	defer tm.kmu.Unlock(taskID)

	seedTask, ok := tm.getTask(taskID)
	if !ok {
		return nil, errTaskNotFound
	}
	tm.accessTimeMap.Store(taskID, time.Now())
	return seedTask.Pieces, nil
}

func (tm *manager) Exist(taskID string) (*SeedTask, bool) {
	return tm.getTask(taskID)
}

func (tm *manager) Delete(taskID string) {
	tm.kmu.Lock(taskID)
	defer tm.kmu.Unlock(taskID)

	tm.deleteTask(taskID)
}

const (
	// gcTasksTimeout specifies the timeout for tasks gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	gcTasksTimeout = 2.0 * time.Second
)

func (tm *manager) GC() error {
	logger.GcLogger.Info("start the task meta gc job")
	startTime := time.Now()

	totalTaskNums := 0
	removedTaskCount := 0
	tm.accessTimeMap.Range(func(key, value interface{}) bool {
		totalTaskNums++
		taskID := key.(string)
		tm.kmu.Lock(taskID)
		defer tm.kmu.Unlock(taskID)
		atime := value.(time.Time)
		if time.Since(atime) < tm.config.ExpireTime {
			return true
		}

		// gc task memory data
		logger.GcLogger.With("type", "meta").Infof("gc task: start to deal with task: %s", taskID)
		tm.deleteTask(taskID)
		removedTaskCount++
		return true
	})

	// slow GC detected, report it with a log warning
	if timeDuring := time.Since(startTime); timeDuring > gcTasksTimeout {
		logger.GcLogger.With("type", "meta").Warnf("gc tasks: %d cost: %.3f", removedTaskCount, timeDuring.Seconds())
	}
	logger.GcLogger.With("type", "meta").Infof("%d tasks were successfully cleared, leaving %d tasks remaining", removedTaskCount,
		totalTaskNums-removedTaskCount)
	return nil
}
