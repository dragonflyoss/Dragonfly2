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
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	scheduler2 "d7y.io/dragonfly/v2/scheduler/scheduler"
	"io"
)

type Client struct {
	client    scheduler.Scheduler_ReportPieceResultServer
	worker    IWorker
	scheduler *scheduler2.Scheduler
}

func CreateClient(client scheduler.Scheduler_ReportPieceResultServer, worker IWorker, scheduler *scheduler2.Scheduler) *Client {
	c := &Client{
		client:    client,
		worker:    worker,
		scheduler: scheduler,
	}
	return c
}

func (c *Client) Start() error {
	return c.doWork()
}

func (c *Client) doWork() error {
	pr, err := c.client.Recv()
	if err == io.EOF {
		return nil
	} else if err != nil {
		// TODO error
		return nil
	}
	pid := pr.SrcPid
	peerTask, _ := mgr.GetPeerTaskManager().GetPeerTask(pid)
	if peerTask == nil {
		peerResult := &scheduler.PeerPacket{
			State:         &base.ResponseState{
				Success: false,
				Code:    dfcodes.PeerTaskNotRegistered,
				Msg:     "peer task not registered",
			},
		}
		c.client.Send(peerResult)
		return nil
	}
	peerTask.SetClient(c.client)

	for {
		if pr != nil {
			logger.Debugf("[%s][%s]: receive a pieceResult %v - %v", pr.TaskId, pr.SrcPid, pr.PieceNum, pr.Success)
		}
		c.worker.ReceiveUpdatePieceResult(pr)
		pr, err = c.client.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			// TODO error
			return nil
		}
	}
}
