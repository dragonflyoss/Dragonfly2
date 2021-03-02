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
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/struct/syncmap"
	"d7y.io/dragonfly/v2/pkg/util/metricsutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

func init() {
	// Ensure that Manager implements the SeedTaskMgr interface
	var manager *Manager = nil
	var _ mgr.SeedTaskMgr = manager
}

type metrics struct {
	tasks                  *prometheus.GaugeVec
	tasksRegisterCount     *prometheus.CounterVec
	tasksRegisterFailCount *prometheus.CounterVec
	triggerCdnCount        *prometheus.CounterVec
	triggerCdnFailCount    *prometheus.CounterVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		tasks: metricsutils.NewGauge(config.SubsystemCdnSystem, "tasks",
			"Current status of cdn tasks", []string{"taskStatus"}, register),

		tasksRegisterCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "seed_tasks_register_total",
			"Total times of registering tasks", []string{}, register),

		tasksRegisterFailCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "seed_tasks_register_failed_total",
			"Total failure times of registering tasks", []string{}, register),

		triggerCdnCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_trigger_total",
			"Total times of triggering cdn", []string{}, register),

		triggerCdnFailCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "cdn_trigger_failed_total",
			"Total failure times of triggering cdn", []string{}, register),
	}
}

// Manager is an implementation of the interface of TaskMgr.
type Manager struct {
	cfg                     *config.Config
	metrics                 *metrics
	taskStore               *syncmap.SyncMap
	accessTimeMap           *syncmap.SyncMap
	taskURLUnReachableStore *syncmap.SyncMap
	resourceClient          source.ResourceClient
	cdnMgr                  mgr.CDNMgr
	progressMgr             mgr.SeedProgressMgr
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr, progressMgr mgr.SeedProgressMgr,
	resourceClient source.ResourceClient,  register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:                     cfg,
		metrics:                 newMetrics(register),
		taskStore:               syncmap.NewSyncMap(),
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		resourceClient:          resourceClient,
		cdnMgr:                  cdnMgr,
		progressMgr:             progressMgr,
	}, nil
}

func (tm *Manager) Register(ctx context.Context, req *types.TaskRegisterRequest) (pieceChan <-chan *types.SeedPiece,
	err error) {
	tm.metrics.tasksRegisterCount.WithLabelValues().Inc()
	pieceChan, err = tm.doRegister(ctx, req)
	if err != nil {
		tm.metrics.tasksRegisterFailCount.WithLabelValues().Inc()
	}
	return
}

func (tm *Manager) doRegister(ctx context.Context, req *types.TaskRegisterRequest) (pieceChan <-chan *types.
	SeedPiece, err error) {
	task, err := tm.addOrUpdateTask(ctx, req)
	if err != nil {
		logger.WithTaskID(req.TaskId).Infof("failed to add or update task with req %+v: %v", req, err)
		return nil, err
	}
	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(task.TaskId, time.Now()); err != nil {
		logger.WithTaskID(task.TaskId).Warnf("failed to update accessTime: %v", err)
	}
	logger.WithTaskID(task.TaskId).Debugf("success to get task info: %+v", task)

	// trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "failed to trigger cdn")
	}
	// watch seed progress
	return tm.progressMgr.WatchSeedProgress(ctx, task.TaskId)
}

// triggerCdnSyncAction
func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTask) error {
	if !isFrozen(task.CdnStatus) {
		logger.WithTaskID(task.TaskId).Infof("seedTask is running or has been downloaded successfully, status:%s", task.CdnStatus)
		return nil
	}
	if isWait(task.CdnStatus) {
		tm.progressMgr.InitSeedProgress(ctx, task.TaskId)
		logger.WithTaskID(task.TaskId).Infof("success to init seed progress")
	}

	updatedTask, err := tm.updateTask(task.TaskId, &types.SeedTask{
		CdnStatus: types.TaskInfoCdnStatusRUNNING,
	})
	if err != nil {
		return errors.Wrapf(err, "update task failed")
	}
	// triggerCDN goroutine
	go func() {
		tm.metrics.triggerCdnCount.WithLabelValues().Inc()
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		if err != nil {
			tm.metrics.triggerCdnFailCount.WithLabelValues().Inc()
			logger.WithTaskID(task.TaskId).Errorf("trigger cdn get error: %v", err)
		}
		go tm.progressMgr.PublishTask(ctx, task.TaskId, updateTaskInfo)
		updatedTask, err = tm.updateTask(task.TaskId, updateTaskInfo)
		if err != nil {
			logger.WithTaskID(task.TaskId).Errorf("update task fail:%v", err)
		} else {
			logger.WithTaskID(task.TaskId).Infof("success to update task cdn updatedTask:%+v", updatedTask)
		}
	}()
	logger.WithTaskID(task.TaskId).Infof("success to start cdn trigger")
	return nil
}

func (tm *Manager) getTask(taskId string) (*types.SeedTask, error) {
	if stringutils.IsBlank(taskId) {
		return nil, errors.Wrap(cdnerrors.ErrEmptyValue, "taskId is empty")
	}

	v, err := tm.taskStore.Get(taskId)
	if err != nil {
		return nil, err
	}
	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(taskId, time.Now()); err != nil {
		logger.WithTaskID(taskId).Warnf("failed to update accessTime: %v", err)
	}
	// type assertion
	if info, ok := v.(*types.SeedTask); ok {
		return info, nil
	}
	return nil, errors.Wrapf(cdnerrors.ErrConvertFailed, "taskId %s: %v", taskId, v)
}

func (tm Manager) Get(ctx context.Context, taskId string) (*types.SeedTask, error) {
	// todo locker
	return tm.getTask(taskId)
}

func (tm Manager) GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error) {
	return tm.accessTimeMap, nil
}

func (tm Manager) Delete(ctx context.Context, taskId string) error {
	tm.accessTimeMap.Delete(taskId)
	tm.taskURLUnReachableStore.Delete(taskId)
	tm.taskStore.Delete(taskId)
	return nil
}

func (tm *Manager) GetPieces(ctx context.Context, taskId string) (pieces []*types.SeedPiece, err error) {
	return tm.progressMgr.GetPieces(ctx, taskId)
}
