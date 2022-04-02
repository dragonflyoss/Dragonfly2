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

//go:generate mockgen -destination ./mocks/mock_cdn_service.go -package mocks d7y.io/dragonfly/v2/cdn/supervisor CDNService

package supervisor

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/progress"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/synclock"
)

var (
	// errResourcesLacked represents a lack of resources, for example, the disk does not have enough space.
	errResourcesLacked = errors.New("resources lacked")
)

func IsResourcesLacked(err error) bool {
	return errors.Is(err, errResourcesLacked)
}

type CDNService interface {
	// RegisterSeedTask registers seed task
	RegisterSeedTask(ctx context.Context, clientAddr string, registerTask *task.SeedTask) (*task.SeedTask, <-chan *task.PieceInfo, error)

	// GetSeedPieces returns pieces associated with taskID, which are sorted by pieceNum
	GetSeedPieces(taskID string) (pieces []*task.PieceInfo, err error)

	// GetSeedTask returns seed task associated with taskID
	GetSeedTask(taskID string) (seedTask *task.SeedTask, err error)

	// AddTaskGCSubscriber add task GC subscriber
	AddTaskGCSubscriber(taskID string, instance *task.GCSubscriberInstance)
}

type cdnService struct {
	taskManager     task.Manager
	cdnManager      cdn.Manager
	progressManager progress.Manager
}

func NewCDNService(taskManager task.Manager, cdnManager cdn.Manager, progressManager progress.Manager) (CDNService, error) {
	return &cdnService{
		taskManager:     taskManager,
		cdnManager:      cdnManager,
		progressManager: progressManager,
	}, nil
}

func (service *cdnService) RegisterSeedTask(ctx context.Context, clientAddr string, registerTask *task.SeedTask) (*task.SeedTask,
	<-chan *task.PieceInfo, error) {
	seedTask, err := service.taskManager.AddOrUpdate(registerTask)
	if err != nil {
		return nil, nil, err
	}
	if err = service.triggerCdnSyncAction(ctx, registerTask.ID); err != nil {
		return seedTask, nil, err
	}
	pieceChan, err := service.progressManager.WatchSeedProgress(ctx, clientAddr, registerTask.ID)
	return seedTask, pieceChan, err
}

// triggerCdnSyncAction trigger cdn sync action
func (service *cdnService) triggerCdnSyncAction(ctx context.Context, taskID string) error {
	seedTask, err := service.taskManager.Get(taskID)
	if err != nil {
		return err
	}
	synclock.Lock(taskID, true)
	if seedTask.SourceFileLength > 0 {
		if ok, err := service.cdnManager.TryFreeSpace(seedTask.SourceFileLength); err != nil {
			seedTask.Log().Errorf("failed to try free space: %v", err)
		} else if !ok {
			synclock.UnLock(taskID, true)
			return errResourcesLacked
		}
	}
	if !seedTask.IsFrozen() {
		seedTask.Log().Infof("seedTask status is %s，no need trigger again", seedTask.CdnStatus)
		synclock.UnLock(seedTask.ID, true)
		return nil
	}
	synclock.UnLock(seedTask.ID, true)

	synclock.Lock(seedTask.ID, false)
	defer synclock.UnLock(seedTask.ID, false)
	// reconfirm
	if !seedTask.IsFrozen() {
		seedTask.Log().Infof("reconfirm seedTask status is not frozen, no need trigger again, current status: %s", seedTask.CdnStatus)
		return nil
	}
	seedTask.StartTrigger()
	// triggerCDN goroutine
	go func() {
		updateTaskInfo, err := service.cdnManager.TriggerCDN(context.Background(), seedTask)
		if err != nil {
			seedTask.Log().Errorf("failed to trigger cdn: %v", err)
		}
		jsonTaskInfo, err := json.Marshal(updateTaskInfo)
		if err != nil {
			seedTask.Log().Errorf("failed to json marshal updateTaskInfo: %#v: %v", updateTaskInfo, err)
			return
		}
		seedTask.Log().Infof("trigger cdn result: %s", jsonTaskInfo)
	}()
	return nil
}

func (service *cdnService) GetSeedPieces(taskID string) ([]*task.PieceInfo, error) {
	return service.taskManager.GetProgress(taskID)
}

func (service *cdnService) GetSeedTask(taskID string) (*task.SeedTask, error) {
	return service.taskManager.Get(taskID)
}

func (service *cdnService) AddTaskGCSubscriber(taskID string, instance *task.GCSubscriberInstance) {
	service.taskManager.AddGCSubscriber(taskID, instance)
}
