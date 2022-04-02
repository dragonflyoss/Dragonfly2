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
	"sync"

	"d7y.io/dragonfly/v2/cdn/config"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type GCSubscriber interface {
	// GC Clean up the taskID
	GC(taskID string)

	// AddGCSubscriberInstance add gcSubscriber instance for taskID
	AddGCSubscriberInstance(taskID string, instance *GCSubscriberInstance)
}

func NewNotifySchedulerTaskGCSubscriber(dynconfig config.DynconfigInterface) (GCSubscriber, error) {
	gcSubscriber := &notifySchedulerGCSubscriber{
		scheduler:       map[string]string{},
		taskSubscribers: map[string][]*GCSubscriberInstance{},
	}
	dynconfig.Register(gcSubscriber)
	return gcSubscriber, nil
}

type notifySchedulerGCSubscriber struct {
	locker          sync.Mutex
	scheduler       map[string]string
	taskSubscribers map[string][]*GCSubscriberInstance
}

func (sub *notifySchedulerGCSubscriber) GC(taskID string) {
	if subs, ok := sub.taskSubscribers[taskID]; ok {
		for _, s := range subs {
			if schedulerTarget, ok := sub.scheduler[s.ClientAddr]; ok {
				clientConn, err := grpc.Dial(schedulerTarget)
				if err != nil {
					logger.Errorf("")
				}
				if err, _ := scheduler.NewSchedulerClient(clientConn).LeaveTask(context.Background(), &scheduler.PeerTarget{
					TaskId: taskID,
					PeerId: s.PeerID,
				}); err != nil {
					logger.Errorf("")
				}
			}
		}
	}
}

func (sub *notifySchedulerGCSubscriber) AddGCSubscriberInstance(taskID string, instance *GCSubscriberInstance) {
	sub.locker.Lock()
	subs, ok := sub.taskSubscribers[taskID]
	if ok {
		subs = append(subs, instance)
		sub.taskSubscribers[taskID] = subs
	}
}

func (sub *notifySchedulerGCSubscriber) OnNotify(data interface{}) {

}
