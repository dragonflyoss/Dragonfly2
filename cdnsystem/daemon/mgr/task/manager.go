package task

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	dutil "github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/util"
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

var _ mgr.TaskMgr = &Manager{}

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
	cfg          *config.Config
	metrics      *metrics
	sourceClient source.SourceClient

	// store object
	taskStore               *dutil.Store
	accessTimeMap           *syncmap.SyncMap
	taskURLUnReachableStore *syncmap.SyncMap

	// mgr object
	cdnMgr       mgr.CDNMgr
}

func (m Manager) AddOrUpdateTask(ctx context.Context, req *types.CdnTaskCreateRequest) (*types.CdnTaskInfo, error) {
	taskId := req.TaskID
	util.GetLock(taskId, true)
	defer util.ReleaseLock(taskId, true)

	if key, err := m.taskURLUnReachableStore.Get(taskId); err == nil {
		if unReachableStartTime, ok := key.(time.Time); ok &&
			time.Since(unReachableStartTime) < m.cfg.FailAccessInterval {
			return nil, errors.Wrapf(errortypes.ErrURLNotReachable, "taskID: %s task hit unReachable cache and interval less than %d, url: %s", taskId, m.cfg.FailAccessInterval, req.URL)
		}

		m.taskURLUnReachableStore.Delete(taskId)
	}

	// using the existing task if it already exists corresponding to taskID
	var task *types.CdnTaskInfo
	newTask := &types.CdnTaskInfo{
		TaskID:     taskId,
		Headers:    req.Headers,
		Md5:        req.Md5,
		Url:        req.URL,
		CdnStatus:  types.TaskInfoCdnStatusWAITING,
		PieceTotal: -1,
	}

	if v, err := m.taskStore.Get(taskId); err == nil {
		task = v.(*types.CdnTaskInfo)
		if !equalsTask(task, newTask) {
			return nil, errors.Wrapf(errortypes.ErrTaskIDDuplicate, "%s", task.TaskID)
		}
	} else {
		task = newTask
	}

	if task.SourceFileLength != 0 {
		return task, nil
	}

	// get sourceContentLength with req.Headers
	sourceFileLength, err := m.sourceClient.GetContentLength(task.Url, req.Headers)
	if err != nil {
		logrus.Errorf("taskID: %s failed to get file length from http client : %v", task.TaskID, err)

		if errortypes.IsURLNotReachable(err) {
			m.taskURLUnReachableStore.Add(taskId, time.Now())
			return nil, err
		}
		if errortypes.IsAuthenticationRequired(err) {
			return nil, err
		}
	}
	// source cdn
	if m.cfg.CDNPattern == config.CDNPatternSource {
		if sourceFileLength <= 0 {
			return nil, fmt.Errorf("taskID: %s failed to get file length and it is required in source CDN pattern", task.TaskID)
		}

		supportRange, err := m.sourceClient.IsSupportRange(task.Url, task.Headers)
		if err != nil {
			return nil, errors.Wrapf(err, "taskID: %s failed to check whether the task supports partial requests", task.TaskID)
		}
		if !supportRange {
			return nil, fmt.Errorf("taskID: %s the task URL should support range request in source CDN pattern", task.TaskID)
		}
	}
	task.SourceFileLength = sourceFileLength
	logrus.Infof("taskID: %s get file length %d from http client for task", task.TaskID, sourceFileLength)

	// if success to get the information successfully with the req.Headers,
	// and then update the task.Headers to req.Headers.
	if req.Headers != nil {
		task.Headers = req.Headers
	}

	// calculate piece size and update the PieceSize and PieceTotal
	pieceSize := computePieceSize(task.SourceFileLength)
	task.PieceSize = pieceSize
	task.PieceTotal = int32((sourceFileLength + (int64(pieceSize) - 1)) / int64(pieceSize))

	m.taskStore.Put(task.TaskID, task)
	// update accessTime for taskID
	if err := m.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		logrus.Warnf("taskID: %s failed to update accessTime for task: %v", task.TaskID, err)
	}
	m.metrics.tasks.WithLabelValues(task.CdnStatus).Inc()
	return task, nil
}

func (m *Manager) TriggerCdnSyncAction(ctx context.Context, task *types.CdnTaskInfo) error {
	if !isFrozen(task.CdnStatus) {
		logrus.Infof("TaskID: %s CDN(%s) is running or has been downloaded successfully", task.TaskID, task.CdnStatus)
		return nil
	}

	if err := m.updateTask(task.TaskID, &types.CdnTaskInfo{
		CdnStatus: types.TaskInfoCdnStatusRUNNING,
	}); err != nil {
		return err
	}

	go func() {
		updateTaskInfo, err := m.cdnMgr.TriggerCDN(ctx, task)
		m.metrics.triggerCdnCount.WithLabelValues().Inc()
		if err != nil {
			m.metrics.triggerCdnFailCount.WithLabelValues().Inc()
			logrus.Errorf("taskID: %s trigger cdn get error: %v", task.TaskID, err)
		}
		m.updateTask(task.TaskID, updateTaskInfo)
		logrus.Infof("taskID: %s success to update task cdn %+v", task.TaskID, updateTaskInfo)
	}()
	logrus.Infof("taskID: %s success to start cdn trigger", task.TaskID)
	return nil
}

func (m *Manager) getTask(taskID string) (*types.CdnTaskInfo, error) {
	if stringutils.IsEmptyStr(taskID) {
		return nil, errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	v, err := m.taskStore.Get(taskID)
	if err != nil {
		return nil, err
	}

	// type assertion
	if info, ok := v.(*types.CdnTaskInfo); ok {
		return info, nil
	}
	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "taskID %s: %v", taskID, v)
}

func (m Manager) Get(ctx context.Context, taskID string) (*types.CdnTaskInfo, error) {
	return m.getTask(taskID)
}

func (m Manager) GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error) {
	return m.accessTimeMap, nil
}

func (m Manager) Delete(ctx context.Context, taskID string) error {
	m.accessTimeMap.Delete(taskID)
	m.taskURLUnReachableStore.Delete(taskID)
	m.taskStore.Delete(taskID)
	return nil
}

func (m Manager) GetPieces(ctx context.Context, taskID, clientID string, piecePullRequest *types.PiecePullRequest) (isFinished bool, data interface{}, err error) {
	panic("implement me")
}

func (m Manager) UpdatePieceStatus(ctx context.Context, taskID, pieceRange string, pieceUpdateRequest *types.PieceUpdateRequest) error {
	panic("implement me")
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr,
	sourceClient source.SourceClient, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:                     cfg,
		taskStore:               dutil.NewStore(),
		cdnMgr:                  cdnMgr,
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		sourceClient:            sourceClient,
		metrics:                 newMetrics(register),
	}, nil
}
