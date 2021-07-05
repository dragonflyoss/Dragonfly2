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
	"io"
	"time"

	"d7y.io/dragonfly/v2/internal/rpc/base/common"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/service"
)

type Client struct {
	client           scheduler.Scheduler_ReportPieceResultServer
	stop             bool
	worker           Worker
	schedulerService *service.SchedulerService
}

func NewClient(client scheduler.Scheduler_ReportPieceResultServer, worker Worker, schedulerService *service.SchedulerService) *Client {
	c := &Client{
		client:           client,
		worker:           worker,
		schedulerService: schedulerService,
	}
	return c
}

func (c *Client) Serve() error {
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
	peerTask, _ := c.schedulerService.TaskManager.PeerTask.Get(pid)
	if peerTask == nil {
		peerResult := &scheduler.PeerPacket{
			Code: dfcodes.PeerTaskNotFound,
		}
		c.client.Send(peerResult)
		return nil
	}
	peerTask.SetClient(c)
	c.schedulerService.CDNManager.AddToCallback(peerTask)

	for !c.stop {
		if pr != nil {
			if pr.PieceNum == common.ZeroOfPiece {
				logger.Infof("[%s][%s]: receive a start signal pieceResult, cost[%d]",
					pr.TaskId, pr.SrcPid, pr.EndTime-pr.BeginTime)
			} else {
				logger.Infof("[%s][%s]: receive a pieceResult %v - %v cost[%d]",
					pr.TaskId, pr.SrcPid, pr.PieceNum, pr.Code, pr.EndTime-pr.BeginTime)
			}
		}
		if pr.PieceNum == common.EndOfPiece {
			logger.Infof("[%s][%s]: client closed total cost[%d]", pr.TaskId, pr.SrcPid, time.Now().UnixNano()-peerTask.GetStartTime())
			return nil
		}
		c.worker.ReceivePieceResult(pr)
		pr, err = c.client.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			// TODO error
			return nil
		}
	}
	return nil
}

func (c *Client) Send(p *scheduler.PeerPacket) error {
	return c.client.Send(p)
}

func (c *Client) Recv() (*scheduler.PieceResult, error) {
	return c.client.Recv()
}

func (c *Client) Close() {
	c.stop = true
}

func (c *Client) IsClosed() bool {
	if c.stop {
		return true
	}
	err := c.client.Context().Err()
	if err != nil {
		return true
	}
	return false
}
