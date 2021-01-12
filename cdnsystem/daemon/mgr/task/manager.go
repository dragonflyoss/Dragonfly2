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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/metricsutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

var _ mgr.SeedTaskMgr = &Manager{}

type metrics struct {
	tasks               *prometheus.GaugeVec
	tasksRegisterCount  *prometheus.CounterVec
	triggerCdnCount     *prometheus.CounterVec
	triggerCdnFailCount *prometheus.CounterVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		tasks: metricsutils.NewGauge(config.SubsystemCdnSystem, "tasks",
			"Current status of cdn tasks", []string{"taskStatus"}, register),

		tasksRegisterCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "seed_tasks_registered_total",
			"Total times of registering tasks", []string{}, register),

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
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr, resourceClient source.ResourceClient, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:                     cfg,
		metrics:                 newMetrics(register),
		taskStore:               syncmap.NewSyncMap(),
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		resourceClient:          resourceClient,
		cdnMgr:                  cdnMgr,
	}, nil
}

// Register register seed task
func (tm *Manager) Register(ctx context.Context, req *types.TaskRegisterRequest) (pieceCh <-chan *types.SeedPiece, err error) {
	task, err := tm.addOrUpdateTask(ctx, req)
	if err != nil {
		logger.Named(task.TaskID).Infof("failed to add or update task with req %+v: %v", req, err)
		return nil, err
	}
	// update accessTime for taskID
	if err := tm.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		logger.Named(task.TaskID).Warnf("failed to update accessTime: %v", err)
	}
	tm.metrics.tasksRegisterCount.WithLabelValues().Inc()
	logger.Named(task.TaskID).Debugf("success to get task info: %+v", task)

	// trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "failed to trigger cdn")
	}
	// watch seed progress
	return tm.cdnMgr.WatchSeedProgress(ctx, task.TaskID)
}

// triggerCdnSyncAction
func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTask) error {
	if !isFrozen(task.CdnStatus) {
		logger.Named(task.TaskID).Infof("seedTask is running or has been downloaded successfully, status:%s", task.CdnStatus)
		return nil
	}
	if isWait(task.CdnStatus) {
		err := tm.cdnMgr.InitSeedProgress(ctx, task.TaskID)
		if err != nil {
			return errors.Wrap(err, "failed to init seed progress")
		}
		logger.Named(task.TaskID).Infof("success to init seed progress")
	}

	updatedTask, err := tm.updateTask(task.TaskID, &types.SeedTask{
		CdnStatus: types.TaskInfoCdnStatusRUNNING,
	})
	if err != nil {
		return errors.Wrapf(err, "update task failed")
	}
	// triggerCDN goroutine
	go func() {
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		tm.metrics.triggerCdnCount.WithLabelValues().Inc()
		if err != nil {
			tm.metrics.triggerCdnFailCount.WithLabelValues().Inc()
			logger.Named(task.TaskID).Errorf("trigger cdn get error: %v", err)
		}
		updatedTask, err = tm.updateTask(task.TaskID, updateTaskInfo)
		if err != nil {
			logger.Named(task.TaskID).Errorf("update task fail:%v", err)
		} else {
			logger.Named(task.TaskID).Infof("success to update task cdn updatedTask:%+v", updatedTask)
		}
	}()
	logger.Named(task.TaskID).Infof("success to start cdn trigger")

	return nil
}

func (tm *Manager) getTask(taskID string) (*types.SeedTask, error) {
	if stringutils.IsEmptyStr(taskID) {
		return nil, errors.Wrap(dferrors.ErrEmptyValue, "taskID is empty")
	}

	v, err := tm.taskStore.Get(taskID)
	if err != nil {
		return nil, err
	}

	// type assertion
	if info, ok := v.(*types.SeedTask); ok {
		return info, nil
	}
	return nil, errors.Wrapf(dferrors.ErrConvertFailed, "taskID %s: %v", taskID, v)
}

func (tm Manager) Get(ctx context.Context, taskID string) (*types.SeedTask, error) {
	return tm.getTask(taskID)
}

func (tm Manager) GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error) {
	return tm.accessTimeMap, nil
}

func (tm Manager) Delete(ctx context.Context, taskID string) error {
	tm.accessTimeMap.Delete(taskID)
	tm.taskURLUnReachableStore.Delete(taskID)
	tm.taskStore.Delete(taskID)
	return nil
}

func (tm *Manager) GetPieces(ctx context.Context, taskID string) (pieces []*types.SeedPiece, err error) {
	return tm.cdnMgr.GetPieces(ctx, taskID)
}
