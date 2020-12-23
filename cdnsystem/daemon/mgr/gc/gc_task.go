package gc

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"sync"
	"time"
)

const (
	// gcTasksTimeout specifies the timeout for tasks gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	gcTasksTimeout = 2.0 * time.Second
)

func (gcm *Manager) gcTasks(ctx context.Context) {
	var removedTaskCount int
	startTime := time.Now()

	// get all taskIDs and the corresponding accessTime
	taskAccessMap, err := gcm.taskMgr.GetAccessTime(ctx)
	if err != nil {
		logger.Errorf("gc tasks: failed to get task accessTime map for GC: %v", err)
		return
	}

	// range all tasks and determine whether they are expired
	taskIDs := taskAccessMap.ListKeyAsStringSlice()
	totalTaskNums := len(taskIDs)
	for _, taskID := range taskIDs {
		atime, err := taskAccessMap.GetAsTime(taskID)
		if err != nil {
			logger.Errorf("gc tasks: failed to get access time taskID(%s): %v", taskID, err)
			continue
		}
		if time.Since(atime) < gcm.cfg.TaskExpireTime {
			continue
		}
		// gc task memory data
		gcm.gcTask(ctx, taskID, false)
		removedTaskCount++
	}

	// slow GC detected, report it with a log warning
	if timeDuring := time.Since(startTime); timeDuring > gcTasksTimeout {
		logger.Warnf("gc tasks:%d cost:%.3f", removedTaskCount, timeDuring.Seconds())
	}

	gcm.metrics.gcTasksCount.WithLabelValues().Add(float64(removedTaskCount))

	logger.Infof("gc tasks: success to full gc task count(%d), remainder count(%d)", removedTaskCount, totalTaskNums-removedTaskCount)
}

func (gcm *Manager) gcTask(ctx context.Context, taskID string, full bool) {
	logger.Infof("gc task: start to deal with task: %s", taskID)

	util.GetLock(taskID, false)
	defer util.ReleaseLock(taskID, false)

	var wg sync.WaitGroup
	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		gcm.gcCDNByTaskID(ctx, taskID, full)
		wg.Done()
	}(&wg)
	// delete memory data
	go func(wg *sync.WaitGroup) {
		gcm.gcTaskByTaskID(ctx, taskID)
		wg.Done()
	}(&wg)

	wg.Wait()
}


func (gcm *Manager) gcCDNByTaskID(ctx context.Context, taskID string, full bool) {
	if err := gcm.cdnMgr.Delete(ctx, taskID, full); err != nil {
		logger.Errorf("gc task: failed to gc cdn meta taskID(%s) full(%t): %v", taskID, full, err)
	}
}

func (gcm *Manager) gcTaskByTaskID(ctx context.Context, taskID string) {
	if err := gcm.taskMgr.Delete(ctx, taskID); err != nil {
		logger.Errorf("gc task: failed to gc task info taskID(%s): %v", taskID, err)
	}
}
