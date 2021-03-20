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

package schedule_worker

import (
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/types"
	"fmt"
	"hash/crc32"
	"runtime/debug"
)

type ISender interface {
	Start()
	Stop()
	Send(peerTask *types.PeerTask)
}

type SenderGroup struct {
	senderNum  int
	chanSize   int
	senderList []*Sender
	stopCh     chan struct{}
}

type Sender struct {
	jobChan chan *string
	stopCh  <-chan struct{}
}

func CreateSender() *SenderGroup {
	senderNum := config.GetConfig().Worker.SenderNum
	chanSize := config.GetConfig().Worker.SenderJobPoolSize
	sg := &SenderGroup{
		senderNum: senderNum,
		chanSize:  chanSize,
	}
	return sg
}

func (sg *SenderGroup) Start() {
	sg.stopCh = make(chan struct{})
	for i := 0; i < sg.senderNum; i++ {
		s := &Sender{
			jobChan: make(chan *string, sg.chanSize),
			stopCh:  sg.stopCh,
		}
		s.Start()
		sg.senderList = append(sg.senderList, s)
	}
	logger.Infof("start sender worker : %d", sg.senderNum)
}

func (sg *SenderGroup) Stop() {
	close(sg.stopCh)
	logger.Infof("stop sender worker : %d", sg.senderNum)
}

func (sg *SenderGroup) Send(peerTask *types.PeerTask) {
	sendId := crc32.ChecksumIEEE([]byte(peerTask.Pid)) % uint32(sg.senderNum)
	sg.senderList[sendId].Send(peerTask)
}

func (s *Sender) Send(peerTask *types.PeerTask) {
	s.jobChan <- &peerTask.Pid
}

func (s *Sender) Start() {
	go s.doSend()
}

func (s *Sender) doSend() {
	var err error
	for {
		select {
		case job := <-s.jobChan:
			peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(*job)
			if peerTask == nil {
				break
			}
			func () {
				defer func() {
					e := recover()
					if e != nil {
						debug.PrintStack()
						err = fmt.Errorf("%v", e)
					}
				} ()
				err = peerTask.Send()
			} ()
			if err != nil && err.Error() == "empty client" {
				logger.Warnf("[%s][%s]: client is empty : %v", peerTask.Task.TaskId, peerTask.Pid, err.Error())
				break
			} else if err != nil {
				//TODO error
				logger.Warnf("[%s][%s]: send result failed : %v", peerTask.Task.TaskId, peerTask.Pid, err.Error())
				break
			} else {
				logger.Debugf("[%s][%s]: send result success", peerTask.Task.TaskId, peerTask.Pid)
			}
			if peerTask.Success {
				break
			}

		case <-s.stopCh:
			return
		}
	}
}
