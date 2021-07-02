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

package worker

import (
	"fmt"
	"hash/crc32"
	"runtime/debug"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type SenderPool struct {
	senderNum        int
	jobChan          chan string
	stopCh           <-chan struct{}
	schedulerService *SchedulerService
}

func NewSenderPool(worker *config.SchedulerWorkerConfig, schedulerService *SchedulerService) *SenderPool {
	return &SenderPool{
		senderNum:        worker.SenderNum,
		schedulerService: schedulerService,
	}
}

func (s *SenderPool) Send(peerTask *types.PeerNode) {
	s.jobChan <- peerTask.GetPeerID()
}

func (s *Sender) doSend() {
	var err error
	for {
		select {
		case job := <-s.jobChan:
			peerTask, _ := s.schedulerService.TaskManager.PeerTask.Get(job)
			if peerTask == nil {
				break
			}
			func() {
				defer func() {
					e := recover()
					if e != nil {
						debug.PrintStack()
						err = fmt.Errorf("%v", e)
					}
				}()
				err = peerTask.Send()
			}()
			if err != nil && err.Error() == "empty client" {
				logger.Warnf("[%s][%s]: client is empty : %v", peerTask.Task.TaskID, peerTask.Pid, err.Error())
				break
			} else if err != nil {
				//TODO error
				logger.Warnf("[%s][%s]: send result failed : %v", peerTask.Task.TaskID, peerTask.Pid, err.Error())
				break
			} else {
				logger.Debugf("[%s][%s]: send result success", peerTask.Task.TaskID, peerTask.Pid)
			}
			if peerTask.Success {
				break
			}

		case <-s.stopCh:
			return
		}
	}
}
