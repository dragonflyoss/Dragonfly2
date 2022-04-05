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
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/cdn/dynconfig"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerGRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type GCSubscriber interface {
	dynconfig.Observer
	// GC Clean up the taskID
	GC(taskID string)

	// AddGCSubscriberInstance add gcSubscriber instance for taskID
	AddGCSubscriberInstance(taskID string, instance *GCSubscriberInstance)

	// GetSchedulers get schedulers map
	GetSchedulers() map[string]string
}

func NewNotifySchedulerTaskGCSubscriber(dynconfig dynconfig.Interface) (GCSubscriber, error) {
	gcSubscriber := &notifySchedulerGCSubscriber{
		schedulers:      map[string]string{},
		taskSubscribers: map[string][]*GCSubscriberInstance{},
	}
	dynconfig.Register(gcSubscriber)
	return gcSubscriber, nil
}

type notifySchedulerGCSubscriber struct {
	locker          sync.RWMutex
	schedulers      map[string]string
	taskSubscribers map[string][]*GCSubscriberInstance
}

func (sub *notifySchedulerGCSubscriber) GC(taskID string) {
	sub.locker.RLock()
	defer sub.locker.RUnlock()
	if subs, ok := sub.taskSubscribers[taskID]; ok {
		wg := sync.WaitGroup{}
		for _, s := range subs {
			if schedulerTarget, ok := sub.schedulers[s.ClientIP]; ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, _, err := retry.Run(context.Background(), func() (interface{}, bool, error) {
						clientConn, err := grpc.Dial(schedulerTarget, rpc.DefaultClientOpts...)
						if err != nil {
							return nil, false, err
						}
						if _, err := scheduler.NewSchedulerClient(clientConn).LeaveTask(context.Background(), &scheduler.PeerTarget{
							TaskId: taskID,
							PeerId: s.PeerID,
						}); err != nil {
							return nil, false, err
						}
						return nil, true, nil
					}, 0.05, 0.2, 3, nil); err != nil {
						logger.Errorf("notify scheduler %s task %s peerID %s failed: %v", schedulerTarget, taskID, s.PeerID, err)
					}
				}()
			}
		}
		wg.Wait()
		delete(sub.taskSubscribers, taskID)
	}
}

func (sub *notifySchedulerGCSubscriber) AddGCSubscriberInstance(taskID string, instance *GCSubscriberInstance) {
	sub.locker.Lock()
	defer sub.locker.Unlock()
	subs, ok := sub.taskSubscribers[taskID]
	if !ok {
		subs = []*GCSubscriberInstance{}
	}
	subs = append(subs, instance)
	sub.taskSubscribers[taskID] = subs
}

func (sub *notifySchedulerGCSubscriber) OnNotify(data interface{}) {
	cdn, ok := data.(*managerGRPC.CDN)
	if !ok {
		logger.Errorf("dynamic data type is not *manager.CDN")
	}
	schedulers := cdn.Schedulers
	var schedulerMap = make(map[string]string, 2*len(schedulers))
	for _, s := range schedulers {
		schedulerMap[s.Ip] = fmt.Sprintf("%s:%d", s.Ip, s.Port)
	}
	sub.locker.Lock()
	sub.schedulers = schedulerMap
	sub.locker.Unlock()
}

func (sub *notifySchedulerGCSubscriber) GetSchedulers() map[string]string {
	sub.locker.Lock()
	defer sub.locker.Unlock()
	return sub.schedulers
}
