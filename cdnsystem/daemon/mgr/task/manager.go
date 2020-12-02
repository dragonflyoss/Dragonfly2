package task

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/metricsutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/syncmap"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"time"
)

var _ mgr.SeedTaskMgr = &Manager{}

type metrics struct {
	tasks                        *prometheus.GaugeVec
	tasksRegisterCount           *prometheus.CounterVec
	triggerCdnCount              *prometheus.CounterVec
	triggerCdnFailCount          *prometheus.CounterVec
	scheduleDurationMilliSeconds *prometheus.HistogramVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		tasks: metricsutils.NewGauge(config.SubsystemSupernode, "tasks",
			"Current status of Supernode tasks", []string{"cdnstatus"}, register),

		tasksRegisterCount: metricsutils.NewCounter(config.SubsystemSupernode, "tasks_registered_total",
			"Total times of registering tasks", []string{}, register),

		triggerCdnCount: metricsutils.NewCounter(config.SubsystemSupernode, "cdn_trigger_total",
			"Total times of triggering cdn", []string{}, register),

		triggerCdnFailCount: metricsutils.NewCounter(config.SubsystemSupernode, "cdn_trigger_failed_total",
			"Total failure times of triggering cdn", []string{}, register),

		scheduleDurationMilliSeconds: metricsutils.NewHistogram(config.SubsystemSupernode, "schedule_duration_milliseconds",
			"Duration for task scheduling in milliseconds", []string{"peer"},
			prometheus.ExponentialBuckets(0.01, 2, 7), register),
	}
}

// Manager is an implementation of the interface of TaskMgr.
type Manager struct {
	cfg                     *config.Config
	metrics                 *metrics
	sourceClient            source.SourceClient
	taskStore               *syncmap.SyncMap
	accessTimeMap           *syncmap.SyncMap
	taskURLUnReachableStore *syncmap.SyncMap
	cdnMgr                  mgr.CDNMgr
}

func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTaskInfo) error {
	if !isFrozen(task.CdnStatus) {
		logrus.Infof("TaskID: %s seedTask is running or has been downloaded successfully, status:%s", task.TaskID, task.CdnStatus)
		return nil
	}
	if err := tm.updateTask(task.TaskID, &types.SeedTaskInfo{
		CdnStatus: types.TaskInfoCdnStatusRUNNING,
	}); err != nil {
		return err
	}

	go func() {
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		tm.metrics.triggerCdnCount.WithLabelValues().Inc()
		if err != nil {
			tm.metrics.triggerCdnFailCount.WithLabelValues().Inc()
			logrus.Errorf("taskID: %s trigger cdn get error: %v", task.TaskID, err)
		}
		tm.updateTask(task.TaskID, updateTaskInfo)
		logrus.Infof("taskID: %s success to update task cdn %+v", task.TaskID, updateTaskInfo)
	}()
	logrus.Infof("taskID: %s success to start cdn trigger", task.TaskID)
	return nil
}

func (tm *Manager) getTask(taskID string) (*types.SeedTaskInfo, error) {
	if stringutils.IsEmptyStr(taskID) {
		return nil, errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	v, err := tm.taskStore.Get(taskID)
	if err != nil {
		return nil, err
	}

	// type assertion
	if info, ok := v.(*types.SeedTaskInfo); ok {
		return info, nil
	}
	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "taskID %s: %v", taskID, v)
}

func (tm Manager) Get(ctx context.Context, taskID string) (*types.SeedTaskInfo, error) {
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

func (tm Manager) GetPieces(ctx context.Context, taskID, clientID string, piecePullRequest *types.PiecePullRequest) (isFinished bool, data interface{}, err error) {
	panic("implement me")
}

func (tm Manager) UpdatePieceStatus(ctx context.Context, taskID, pieceRange string, pieceUpdateRequest *types.PieceUpdateRequest) error {
	panic("implement me")
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr,
	sourceClient source.SourceClient, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:                     cfg,
		taskStore:               syncmap.NewSyncMap(),
		cdnMgr:                  cdnMgr,
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		sourceClient:            sourceClient,
		metrics:                 newMetrics(register),
	}, nil
}

func (tm *Manager) Register(ctx context.Context,  req *types.TaskRegisterRequest) (taskCreateResponse *types.TaskRegisterResponse, err error) {
	task, err := tm.addOrUpdateTask(ctx, req)
	if err != nil {
		logrus.Infof("failed to add or update task with req %+v: %v", req, err)
		return nil, err
	}
	tm.metrics.tasksRegisterCount.WithLabelValues().Inc()
	logrus.Debugf("success to get task info: %+v", task)
	// 读锁
	util.GetLock(task.TaskID, true)
	defer util.ReleaseLock(task.TaskID, true)

	if err := tm.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		logrus.Warnf("taskID: %s failed to update accessTime for task: %v", task.TaskID, err)
	}
	// update accessTime for taskID
	if err := tm.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		logrus.Warnf("failed to update accessTime for taskID(%s): %v", task.TaskID, err)
	}
	// Step5: trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(errortypes.ErrSystemError, "failed to trigger cdn: %v", err)
	}
	return &types.TaskRegisterResponse{
		SourceFileLength: task.SourceFileLength,
		PieceSize: task.PieceSize,
	}, nil
}
